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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A variant of the URLClassLoader that first loads from the URLs and only after that from the
 * parent.
 *
 * <p>{@link #getResourceAsStream(String)} uses {@link #getResource(String)} internally so we don't
 * override that.
 */
/*
 *                                          BootstrapClassLoader
 *                                                  |
 *                                          ExtClassLoader
 *                                                  |
 *                                          AppClassLoader
 *                                                  |
 *                                    ChildFirstCacheClassLoader(Job)
 *                                   /                                \
 *         ChildFirstCacheClassLoader(Plugin)                   ChildFirstCacheClassLoader(Plugin)
 */
public final class ChildFirstCacheClassLoader extends FlinkUserCodeClassLoader {
    public static Set<String> filterResource = new HashSet<>(6);

    static {
        ClassLoader.registerAsParallelCapable();
        filterResource.add("core-default.xml");
        filterResource.add("core-site.xml");
        filterResource.add("hdfs-default.xml");
        filterResource.add("hdfs-site.xml");
        filterResource.add("yarn-default.xml");
        filterResource.add("yarn-site.xml");
        filterResource.add("mapred-site.xml");
    }

    /**
     * The classes that should always go through the parent ClassLoader. This is relevant for Flink
     * classes, for example, to avoid loading Flink classes that cross the user-code/system-code
     * barrier in the user-code ClassLoader.
     */
    private final String[] alwaysParentFirstPatterns;

    private final String[] onlyJobAlwaysParentFirstPatterns;

    private final Consumer<Throwable> classLoadingExceptionHandler;
    private Map<String, FlinkUserCodeClassLoader> pluginClassLoaderCache;
    private boolean isPluginClassLoader = false;

    private Set<URL> chunjunCommon;

    private URL chunjunCommonUrl;

    private String classLoaderKey;

    private Set<ClassLoader> childClassLoader;

    private String chunjunClassPathPrefix;

    public ChildFirstCacheClassLoader(
            URL[] urls,
            ClassLoader parent,
            String[] alwaysParentFirstPatterns,
            String[] onlyJobAlwaysParentFirstPatterns,
            Consumer<Throwable> classLoadingExceptionHandler,
            boolean isPluginClassLoader,
            String classLoaderKey) {
        super(urls, parent, classLoadingExceptionHandler);
        this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
        this.onlyJobAlwaysParentFirstPatterns = onlyJobAlwaysParentFirstPatterns;
        this.classLoadingExceptionHandler = classLoadingExceptionHandler;
        this.chunjunCommon =
                Arrays.stream(urls)
                        .filter(url -> url.getPath().contains("chunjun-common"))
                        .collect(Collectors.toSet());
        if (chunjunCommon.size() > 0) {
            chunjunCommonUrl = chunjunCommon.iterator().next();
            chunjunClassPathPrefix = chunjunCommonUrl.getPath();
            chunjunClassPathPrefix =
                    chunjunClassPathPrefix.substring(
                            0, chunjunClassPathPrefix.lastIndexOf(File.separator));
        }
        this.isPluginClassLoader = isPluginClassLoader;
        this.classLoaderKey = classLoaderKey;
        childClassLoader = new HashSet<>(2);
    }

    public String getChunjunClassPathPrefix() {
        return chunjunClassPathPrefix;
    }

    public void setChunjunClassPathPrefix(String chunjunClassPathPrefix) {
        this.chunjunClassPathPrefix = chunjunClassPathPrefix;
    }

    public void addChildClassLoader(ClassLoader classLoader) {
        childClassLoader.add(classLoader);
    }

    public Set<ClassLoader> getChildClassLoader() {
        return childClassLoader;
    }

    public String getClassLoaderKey() {
        return classLoaderKey;
    }

    public void setClassLoaderKey(String classLoaderKey) {
        this.classLoaderKey = classLoaderKey;
    }

    private static boolean isPluginConnector(URL resource) {
        // jar:file:/opt/xx/syncplugin/connector/hbase14/chunjun-connector-hbase-1.4-test_beta_1.12_5.0_0824.jar!/com/dtstack/chunjun/connector/hbase14/source/HBase14SourceFactory.class
        return resource != null
                && resource.getPath() != null
                // && resource.getPath().contains("chunjun-connector-");
                && resource.getPath().contains("/com/dtstack/chunjun/connector");
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        try {
            synchronized (getClassLoadingLock(name)) {
                return loadClassWithoutExceptionHandling(name, resolve);
            }
        } catch (FlinkRuntimeException e) {
            throw e;
        } catch (Throwable classLoadingException) {
            classLoadingExceptionHandler.accept(classLoadingException);
            throw classLoadingException;
        }
    }

    @Override
    protected Class<?> loadClassWithoutExceptionHandling(String name, boolean resolve)
            throws ClassNotFoundException, FlinkRuntimeException {

        // First, check if the class has already been loaded
        Class<?> c = findLoadedClass(name);

        if (c == null) {
            // check whether the class should go parent-first
            for (String alwaysParentFirstPattern : alwaysParentFirstPatterns) {
                if (name.startsWith(alwaysParentFirstPattern)) {
                    return super.loadClassWithoutExceptionHandling(name, resolve);
                }
            }

            // Job ClassLoader 且 org.apache.hadoop.fs.FileSystem 实现类用父类加载器加载
            if (!isPluginClassLoader) {
                // check whether the class should go parent-first
                for (String alwaysParentFirstPattern : onlyJobAlwaysParentFirstPatterns) {
                    if (name.startsWith(alwaysParentFirstPattern)) {
                        return super.loadClassWithoutExceptionHandling(name, resolve);
                    }
                }
            }

            try {
                // 根据类名找到 Jar 的 URL 后, 缓存 ClassLoader.
                URL resource = findClassResource(name);
                // eg:
                // jar:file:/opt/xx/syncplugin/connector/hbase14/chunjun-connector-hbase-1.4-test_beta_1.12_5.0_0824.jar!/com/dtstack/chunjun/connector/hbase14/source/HBase14SourceFactory.class
                if (!isPluginClassLoader && isPluginConnector(resource)) {
                    // 创建 chunjun Plugin(Connector) ClassLoader, 并缓存到[类加载器缓存].
                    getOrCreatePluginClassLoader(resource.getPath());
                    // 抛出异常在上一层处理是为了避免父 Classloader 锁后，子 Classloader 再锁会出现死锁。
                    throw new FlinkRuntimeException(resource.getPath());
                } else {
                    // check the URLs
                    c = findClass(name);
                }
            } catch (ClassNotFoundException e) {
                // let URLClassLoader do it, which will eventually call the parent
                c = super.loadClassWithoutExceptionHandling(name, resolve);
            }
        } else if (resolve) {
            resolveClass(c);
        }

        return c;
    }

    public void setIsCacheClassLoader(boolean value) {
        isPluginClassLoader = value;
    }

    public boolean isPluginClassLoader() {
        return isPluginClassLoader;
    }

    public FlinkUserCodeClassLoader getOrCreatePluginClassLoader(String path) {
        String finalPath = ClassLoaderCacheManager.getJarURL(path);
        ArrayList<URL> urlList = new ArrayList<>(2);
        if (chunjunCommonUrl != null) {
            urlList.add(chunjunCommonUrl);
        }
        if (finalPath != null) {
            try {
                urlList.add(new URL(finalPath));
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }
        return ClassLoaderCacheManager.getChunjunPluginClassLoader(
                urlList.toArray(new URL[0]),
                this,
                alwaysParentFirstPatterns,
                onlyJobAlwaysParentFirstPatterns,
                classLoadingExceptionHandler);
    }

    protected URL findClassResource(final String name) {
        String path = name.replace('.', '/').concat(".class");
        return getResource(path);
    }

    @Override
    public URL getResource(String name) {

        // job class loader
        if (!isPluginClassLoader && filterResource.contains(name)) {
            return super.getResource(name);
        }

        // plugin class loader
        // first, try and find it via the URLClassloader
        URL urlClassLoaderResource = findResource(name);

        if (urlClassLoaderResource != null) {
            return urlClassLoaderResource;
        }
        // 过滤 Hadoop 资源文件
        if (filterResource.contains(name)) {
            return null;
        }

        // delegate to super
        return super.getResource(name);
    }

    /*
     *                        BootstrapClassLoader
     *                                |
     *                        ExtClassLoader
     *                                |
     *                        AppClassLoader                                    <---- SPI 不查找资源(/.../xxx.jar!/META-INF/services/xxx)
     *                                |
     *                  ChildFirstCacheClassLoader(Job)                         <---- SPI 不查找资源(/.../xxx.jar!/META-INF/services/xxx)
     *                 /                                \
     *ChildFirstCacheClassLoader(Plugin)  ChildFirstCacheClassLoader(Plugin)    <---- SPI 查找资源(/.../xxx.jar!/META-INF/services/xxx)
     */
    @Override
    public Enumeration<URL> getResources(String name) throws IOException {

        final List<URL> result = new ArrayList<>();

        // Plugin ClassLoader
        if (isPluginClassLoader) {
            // first get resources from URLClassloader
            Enumeration<URL> urlClassLoaderResources = findResources(name);
            while (urlClassLoaderResources.hasMoreElements()) {
                result.add(urlClassLoaderResources.nextElement());
            }
        }

        // Job ClassLoader 且 org.apache.hadoop.fs.FileSystem 不在本级找
        // eg : name = META-INF/services/org.apache.hadoop.fs.FileSystem
        if (!isPluginClassLoader) {
            boolean isParentPattern = true;
            // check whether the class should go parent-first
            for (String alwaysParentFirstPattern : onlyJobAlwaysParentFirstPatterns) {
                if (name.contains(alwaysParentFirstPattern)) {
                    Enumeration<URL> parentResources = getParent().getResources(name);
                    while (parentResources.hasMoreElements()) {
                        result.add(parentResources.nextElement());
                    }
                    isParentPattern = false;
                }
            }
            if (isParentPattern) {

                Enumeration<URL> urlClassLoaderResources = findResources(name);

                while (urlClassLoaderResources.hasMoreElements()) {
                    result.add(urlClassLoaderResources.nextElement());
                }

                // get parent urls
                Enumeration<URL> parentResources = getParent().getResources(name);

                while (parentResources.hasMoreElements()) {
                    result.add(parentResources.nextElement());
                }
            }
        }

        return new Enumeration<URL>() {
            final Iterator<URL> iter = result.iterator();

            public boolean hasMoreElements() {
                return iter.hasNext();
            }

            public URL nextElement() {
                return iter.next();
            }
        };
    }
}
