/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.URLClassPath;

import java.io.File;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public final class ClassLoaderCacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderCacheManager.class);

    private static final String CHUNJUN = "chunjun";

    private static final String MAIN_THREAD_NAME = "main";

    private static final String CLIENT = "client";

    private static final String cluster = "cluster";

    private static boolean isMiniCluster = false;

    public static final ConfigOption<String> OPERATOR_PATH =
            ConfigOptions.key("operatorPath")
                    .stringType()
                    .defaultValue(null)
                    .withDescription("Chunjun connector plugin jar path. eg: ");

    public static final ConfigOption<String> RELATIVE_OPERATOR_PATH =
            ConfigOptions.key("relativeOperatorPath")
                    .stringType()
                    .defaultValue(null)
                    .withDescription("Chunjun connector plugin jar path. eg: ");

    private static ConcurrentHashMap<String, FlinkUserCodeClassLoader> classloaderCache =
            new ConcurrentHashMap<>();

    public static ConcurrentHashMap<String, ConcurrentHashMap<String, FlinkUserCodeClassLoader>>
            miniClusterClassLoaderCacheCache = new ConcurrentHashMap<>();

    public static boolean isIsMiniCluster() {
        return isMiniCluster;
    }

    public static void setIsMiniCluster(boolean isMiniCluster) {
        ClassLoaderCacheManager.isMiniCluster = isMiniCluster;
    }

    public static synchronized FlinkUserCodeClassLoader getChunjunJobClassLoader(
            URL[] urls,
            ClassLoader parent,
            String[] alwaysParentFirstPatterns,
            String[] onlyJobAlwaysParentFirstPatterns,
            Consumer<Throwable> classLoadingExceptionHandler) {

        // 此处使用带缓存的 child-first 方式，根据 reader 和 writer 去获取已经存在
        String classloaderKey = extractClassloaderCacheKey(urls, "(", ")");

        // 替换客户端 classpath 文件缓存路径为 NodeManager 本地 classpath 缓存路径
        replaceClientClasspathToNodeManagerCachePath(urls);

        Map<String, FlinkUserCodeClassLoader> cache = getClusterCache();

        FlinkUserCodeClassLoader loader = cache.get(classloaderKey);
        if (loader == null) {
            loader =
                    new ChildFirstCacheClassLoader(
                            urls,
                            parent,
                            alwaysParentFirstPatterns,
                            onlyJobAlwaysParentFirstPatterns,
                            classLoadingExceptionHandler,
                            false,
                            classloaderKey);
            cache.put(classloaderKey, loader);
            logClassloaderCacheInfo(classloaderKey, cache);
        }

        return loader;
    }

    public static synchronized FlinkUserCodeClassLoader getChunjunPluginClassLoader(
            URL[] urls,
            ChildFirstCacheClassLoader parent,
            String[] alwaysParentFirstPatterns,
            String[] onlyJobAlwaysParentFirstPatterns,
            Consumer<Throwable> classLoadingExceptionHandler) {
        // 特殊处理 MiniCluster
        Map<String, FlinkUserCodeClassLoader> cache = getClusterCache();
        // eg : (mysql_redis)->mysql , (mysql_redis)->redis, (mysql)->mysql
        String classloaderKey =
                extractClassloaderCacheKey(urls, parent.getClassLoaderKey() + "->", "");

        // 替换客户端 classpath 文件缓存路径为 NodeManager 本地 classpath 缓存路径
        replaceClientClasspathToNodeManagerCachePath(urls);

        FlinkUserCodeClassLoader loader = cache.get(classloaderKey);
        if (loader == null) {
            loader =
                    new ChildFirstCacheClassLoader(
                            urls,
                            parent,
                            alwaysParentFirstPatterns,
                            onlyJobAlwaysParentFirstPatterns,
                            classLoadingExceptionHandler,
                            true,
                            classloaderKey);
            parent.addChildClassLoader(loader);
            cache.put(classloaderKey, loader);
            // 打印日志
            logClassloaderCacheInfo(classloaderKey, cache);
        }
        return loader;
    }

    /**
     * 用 operator 创建 plugin classloader
     *
     * @param classLoader
     * @param configuration
     * @return
     */
    public static synchronized ClassLoader getClassLoader(
            ClassLoader classLoader, Configuration configuration) {
        String path;
        if (ClassLoaderCacheManager.isMiniCluster) {
            // 创建插件类加载器要取相对路径，
            path = configuration.getString(ClassLoaderCacheManager.OPERATOR_PATH);
        } else {
            // 非 local 模式要处理, 兼容 MiniCluster
            path = configuration.getString(ClassLoaderCacheManager.RELATIVE_OPERATOR_PATH);
        }
        if (classLoader instanceof ChildFirstCacheClassLoader // job classloader
                && !((ChildFirstCacheClassLoader) classLoader).isPluginClassLoader()
                && path != null) {
            ClassLoader newThreadClassLoader =
                    ((ChildFirstCacheClassLoader) classLoader).getOrCreatePluginClassLoader(path);
            return newThreadClassLoader;
        }
        return classLoader;
    }

    private static ConcurrentHashMap<String, FlinkUserCodeClassLoader> getClusterCache() {
        ConcurrentHashMap<String, FlinkUserCodeClassLoader> cache;
        if (isMiniCluster) {
            // MiniCluster 是在一个 JVM 中，如果区分开 Client、JM、TM 三分部分缓存，需要 new 出来。
            // classloaderCache = new ConcurrentHashMap<>();
            String threadName = Thread.currentThread().getName();
            if (threadName.contains(MAIN_THREAD_NAME)) {
                // Client       ThreadName = "main"
                cache =
                        miniClusterClassLoaderCacheCache.computeIfAbsent(
                                CLIENT, c -> new ConcurrentHashMap<>());
            } else {
                // Client 与 JobManager 和 TaskManger 的 ClassLoader 缓存区分开.
                // JobManager ThreadName = mini-cluster-io-thread-x
                //                       / flink-akka.actor.default-dispatcher-x
                // ("mini-cluster-io-thread-12"@10,229 in group "main": RUNNING)
                // -----------------------------------------------------------
                // TaskManger ThreadName = Source: elasticsearch7sourcefactory -> Sink:
                //                       / flink-akka.actor.default-dispatcher-x
                // streamsinkfactory (1/1)#0
                cache =
                        miniClusterClassLoaderCacheCache.computeIfAbsent(
                                cluster, c -> new ConcurrentHashMap<>());
            }
            return cache;
        }
        return classloaderCache;
    }

    public static String extractClassloaderCacheKey(URL[] urls, String prefix, String suffix) {
        // prefix 主要是为了区分 source 和 sink 同一个 key 导致的 job classloader 和 plugin classloader key 重复。
        String[] pluginNames =
                Arrays.stream(urls)
                        .filter(
                                url ->
                                        url != null
                                                && url.toString().contains("connector")
                                                && url.toString().endsWith(".jar"))
                        .map(URL::toString)
                        .distinct()
                        .sorted()
                        .map(
                                absJar -> {
                                    String jar =
                                            absJar.substring(
                                                    absJar.lastIndexOf(File.separator) + 1);
                                    String pluginName = jar.split("-")[2];
                                    return pluginName;
                                })
                        .toArray(String[]::new);
        if (pluginNames.length == 0) {
            pluginNames =
                    Arrays.stream(urls)
                            .filter(
                                    url ->
                                            url != null
                                                    && url.toString().contains("chunjun-")
                                                    && url.toString().endsWith(".jar"))
                            .map(URL::toString)
                            .distinct()
                            .sorted()
                            .map(
                                    absJar -> {
                                        String jar =
                                                absJar.substring(
                                                        absJar.lastIndexOf(File.separator) + 1);
                                        String pluginName = jar.split("-")[1];
                                        return pluginName;
                                    })
                            .toArray(String[]::new);
        }
        String classloaderKey = StringUtils.join(pluginNames, "_");
        return prefix + classloaderKey + suffix;
    }

    private static void replaceClientClasspathToNodeManagerCachePath(URL[] urls) {
        for (int i = 0; i < urls.length; i++) {
            URL url = urls[i];
            try {
                File classPathFile = new File(url.toURI());
                // classPath 里面的文件不存在
                if (!classPathFile.exists()) {
                    String currentNodeRelativePath = CHUNJUN + url.getPath();
                    File currentNodeJarFile = new File(currentNodeRelativePath);
                    if (currentNodeJarFile.exists()) {
                        try {
                            urls[i] = currentNodeJarFile.toURI().toURL();
                        } catch (MalformedURLException e) {
                            LOG.error("Failed to convert Chunjun plugin path to URL.", e);
                        }
                    } else {
                        // 替换完还不存在
                        LOG.warn("Chunjun plugin path is not exist.", currentNodeJarFile.toPath());
                    }
                }
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }
    }

    public static void logClassloaderCacheInfo(
            String classloaderKey, Map<String, FlinkUserCodeClassLoader> classloaderCache) {
        LOG.info(
                "---------------------------------------------------------------- All Classloader Cache Start ----------------------------------------------------------------------");
        LOG.info("Chunjun new classloader key : {}", classloaderKey);
        classloaderCache.entrySet().stream()
                .forEach(
                        entry -> {
                            try {
                                ChildFirstCacheClassLoader c =
                                        (ChildFirstCacheClassLoader) entry.getValue();
                                Field ucp = URLClassLoader.class.getDeclaredField("ucp");
                                ucp.setAccessible(true);
                                URLClassPath paths = (URLClassPath) ucp.get(c);
                                StringBuilder classpathBuilder = new StringBuilder();
                                classpathBuilder.append("\n");
                                Arrays.stream(paths.getURLs())
                                        .map(URL::toString)
                                        .forEach(url -> classpathBuilder.append(url).append("\n"));
                                LOG.info(
                                        "Classloader Cache : key = [ {} ], value = [ {} ] ",
                                        entry.getKey(),
                                        classpathBuilder);
                            } catch (NoSuchFieldException | IllegalAccessException e) {
                                LOG.warn("Classloader Cache log print fail.", e);
                            }
                        });
        LOG.info(
                "---------------------------------------------------------------- All Classloader Cache End ! ----------------------------------------------------------------------");
    }

    public static void setContextClassLoader(ClassLoader classLoader) {
        if (classLoader instanceof ChildFirstCacheClassLoader) {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    public static String getJarURL(String path) {
        // jar:file:/opt/xxx/syncplugin/connector/hbase14/chunjun-connector-hbase-1.4-test_beta_1.12_5.0_0824.jar!/com/dtstack/chunjun/connector/hbase14/source/HBase14SourceFactory.class
        // ==>
        // jar:file:/opt/xxx/syncplugin/connector/hbase14/chunjun-connector-hbase-1.4-test_beta_1.12_5.0_0824.jar
        // 处理blob 加载的模式
        if (path.contains("!/")) {
            return path.substring(0, path.indexOf("!/"));
        }

        return path.substring(0, path.indexOf("jar") + 3);
    }

    public static FlinkUserCodeClassLoader getPluginClassLoader(
            FlinkRuntimeException e, ClassLoader c) {
        if (!(c instanceof ChildFirstCacheClassLoader)) {
            throw new FlinkRuntimeException(
                    "If child-first-cache mode,  require to set [ classloader.check-leaked-classloader : false ] .",
                    e);
        }
        // 取出上面抛出异常里面存储的插件 URL.
        String flinkxPluginResourcePath = e.getMessage();
        ChildFirstCacheClassLoader childFirstCacheClassLoader = (ChildFirstCacheClassLoader) c;
        FlinkUserCodeClassLoader pluginClassLoader =
                childFirstCacheClassLoader.getOrCreatePluginClassLoader(flinkxPluginResourcePath);
        return pluginClassLoader;
    }
}
