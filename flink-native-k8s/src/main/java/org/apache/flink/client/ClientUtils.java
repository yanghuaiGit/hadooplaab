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

package org.apache.flink.client;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.*;
import org.apache.flink.client.program.rest.retry.ExponentialWaitStrategy;
import org.apache.flink.client.program.rest.retry.WaitStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.shaded.guava18.com.google.common.base.Splitter;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utility functions for Flink client. */
public enum ClientUtils {
    ;

    public static final String CHILD_FIRST_CACHE = "child-first-cache";

    public static final String CHILD_FIRST_LOADER_PATTERNS = "classloader.child-first-patterns";

    public static final String CHILD_FIRST_LOADER_PATTERNS_DEFAULT =
            "org.apache.flink.table.planner.plan.QueryOperationConverter;org.apache.flink.streaming.api.graph.StreamGraph;org.apache.flink.streaming.api.functions.async.RichAsyncFunction;org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;org.apache.flink.table.planner.plan.nodes.common.CommonLookupJoin;org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalTableSourceScan";

    private static final Logger LOG = LoggerFactory.getLogger(ClientUtils.class);

    /** cache classloader */
    private static Map<String, URLClassLoader> cacheClassLoader = new ConcurrentHashMap<>();

    public static URLClassLoader buildUserCodeClassLoader(
            List<URL> jars, List<URL> classpaths, ClassLoader parent, Configuration configuration) {
        URL[] urls = new URL[jars.size() + classpaths.size()];
        for (int i = 0; i < jars.size(); i++) {
            urls[i] = jars.get(i);
        }
        for (int i = 0; i < classpaths.size(); i++) {
            urls[i + jars.size()] = classpaths.get(i);
        }
        final String[] alwaysParentFirstLoaderPatterns =
                CoreOptions.getParentFirstLoaderPatterns(configuration);
        final String classLoaderResolveOrder =
                configuration.getString(CoreOptions.CLASSLOADER_RESOLVE_ORDER);
        final String[] onlyJobAlwaysParentFirstPatterns =
                CoreOptions.getOnlyJobParentFirstLoaderPatterns(configuration);
        FlinkUserCodeClassLoaders.ResolveOrder resolveOrder =
                FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder);
        final boolean checkClassloaderLeak =
                configuration.getBoolean(CoreOptions.CHECK_LEAKED_CLASSLOADER);
        return FlinkUserCodeClassLoaders.create(
                resolveOrder,
                urls,
                parent,
                alwaysParentFirstLoaderPatterns,
                onlyJobAlwaysParentFirstPatterns,
                NOOP_EXCEPTION_HANDLER,
                checkClassloaderLeak);
    }

    public static void executeProgram(
            PipelineExecutorServiceLoader executorServiceLoader,
            Configuration configuration,
            PackagedProgram program,
            boolean enforceSingleJobExecution,
            boolean suppressSysout)
            throws ProgramInvocationException {
        checkNotNull(executorServiceLoader);
        final ClassLoader userCodeClassLoader = program.getUserCodeClassLoader();
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(userCodeClassLoader);

            LOG.info(
                    "Starting program (detached: {})",
                    !configuration.getBoolean(DeploymentOptions.ATTACHED));

            ContextEnvironment.setAsContext(
                    executorServiceLoader,
                    configuration,
                    userCodeClassLoader,
                    enforceSingleJobExecution,
                    suppressSysout);

            StreamContextEnvironment.setAsContext(
                    executorServiceLoader,
                    configuration,
                    userCodeClassLoader,
                    enforceSingleJobExecution,
                    suppressSysout);

            try {
                program.invokeInteractiveModeForExecution();
            } finally {
                ContextEnvironment.unsetAsContext();
                StreamContextEnvironment.unsetAsContext();
            }
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    /**
     * This method blocks until the job status is not INITIALIZING anymore.
     *
     * @param jobStatusSupplier supplier returning the job status.
     * @param jobResultSupplier supplier returning the job result. This will only be called if the
     *     job reaches the FAILED state.
     * @throws JobInitializationException If the initialization failed
     */
    public static void waitUntilJobInitializationFinished(
            SupplierWithException<JobStatus, Exception> jobStatusSupplier,
            SupplierWithException<JobResult, Exception> jobResultSupplier,
            ClassLoader userCodeClassloader)
            throws JobInitializationException {
        LOG.debug("Wait until job initialization is finished");
        WaitStrategy waitStrategy = new ExponentialWaitStrategy(50, 2000);
        try {
            JobStatus status = jobStatusSupplier.get();
            long attempt = 0;
            while (status == JobStatus.INITIALIZING) {
                Thread.sleep(waitStrategy.sleepTime(attempt++));
                status = jobStatusSupplier.get();
            }
            if (status == JobStatus.FAILED) {
                JobResult result = jobResultSupplier.get();
                Optional<SerializedThrowable> throwable = result.getSerializedThrowable();
                if (throwable.isPresent()) {
                    Throwable t = throwable.get().deserializeError(userCodeClassloader);
                    if (t instanceof JobInitializationException) {
                        throw t;
                    }
                }
            }
        } catch (JobInitializationException initializationException) {
            throw initializationException;
        } catch (Throwable throwable) {
            ExceptionUtils.checkInterrupted(throwable);
            throw new RuntimeException("Error while waiting for job to be initialized", throwable);
        }
    }

    /** overroad this method to add timeout */
    public static JobExecutionResult submitJob(
            ClusterClient<?> client, JobGraph jobGraph, long timeout, TimeUnit unit)
            throws ProgramInvocationException, TimeoutException {
        checkNotNull(client);
        checkNotNull(jobGraph);
        try {
            return client.submitJob(jobGraph)
                    .thenApply(DetachedJobExecutionResult::new)
                    .get(timeout, unit);
        } catch (InterruptedException | ExecutionException e) {
            ExceptionUtils.checkInterrupted(e);
            throw new ProgramInvocationException(
                    "Could not run job in detached mode.", jobGraph.getJobID(), e);
        }
    }

    /** overload this method to add cache */
    public static URLClassLoader buildUserCodeClassLoader(
            List<URL> jars,
            List<URL> classpaths,
            ClassLoader parent,
            Configuration configuration,
            boolean cache) {
        URL[] urls = new URL[jars.size() + classpaths.size()];

        for (int i = 0; i < jars.size(); i++) {
            urls[i] = jars.get(i);
        }
        for (int i = 0; i < classpaths.size(); i++) {
            urls[i + jars.size()] = classpaths.get(i);
        }
        final String[] alwaysParentFirstLoaderPatterns =
                CoreOptions.getParentFirstLoaderPatterns(configuration);
        final String[] onlyJobAlwaysParentFirstPatterns =
                CoreOptions.getOnlyJobParentFirstLoaderPatterns(configuration);

        final String[] childFirstLoaderPatternsArray = parseChildFirstLoaderPatterns(configuration);

        final String classLoaderResolveOrder =
                configuration.getString(
                        CoreOptions.CLASSLOADER_RESOLVE_ORDER,
                        CoreOptions.CLASSLOADER_RESOLVE_ORDER.defaultValue());
        FlinkUserCodeClassLoaders.ResolveOrder resolveOrder =
                FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder);
        final boolean checkClassloaderLeak =
                configuration.getBoolean(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);
        URLClassLoader classLoader =
                FlinkUserCodeClassLoaders.create(
                        resolveOrder,
                        urls,
                        parent,
                        alwaysParentFirstLoaderPatterns,
                        onlyJobAlwaysParentFirstPatterns,
                        NOOP_EXCEPTION_HANDLER,
                        checkClassloaderLeak);
        // child-first-cache 在类加载器里面进行了缓存。
        if (CHILD_FIRST_CACHE.equalsIgnoreCase(classLoaderResolveOrder)) {
            return classLoader;
        }
        if (cache) {
            Arrays.sort(urls, Comparator.comparing(URL::toString));
            String[] jarMd5s = new String[urls.length];
            for (int i = 0; i < urls.length; ++i) {
                try (FileInputStream inputStream = new FileInputStream(urls[i].getPath())) {
                    jarMd5s[i] = DigestUtils.md5Hex(inputStream);
                } catch (Exception e) {
                    throw new FlinkRuntimeException("Exceptions appears when read file:" + e);
                }
            }
            String keyCache = classLoaderResolveOrder + StringUtils.join(jarMd5s, "_");
            return cacheClassLoader.computeIfAbsent(keyCache, k -> classLoader);
        } else {
            return classLoader;
        }
    }

    private static String[] parseChildFirstLoaderPatterns(Configuration configuration) {
        Splitter splitter = Splitter.on(';').omitEmptyStrings();
        final String childFirstPatternsStr =
                configuration.getString(
                        CHILD_FIRST_LOADER_PATTERNS, CHILD_FIRST_LOADER_PATTERNS_DEFAULT);
        return Iterables.toArray(splitter.split(childFirstPatternsStr), String.class);
    }
}
