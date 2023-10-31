/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example;


import com.google.common.base.Charsets;
import io.fabric8.kubernetes.api.model.*;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.JarUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.URLClassPath;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.example.RdosTaskStatus.*;

/**
 * Date: 2021/10/22 Company: www.dtstack.com
 *
 * @author tudou
 */
public class KubernetesPluginUtil {
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesPluginUtil.class);
    public static final String tmpK8sConfigDir = "tmpK8sConf";
    public static final Pattern EFFECTIVE_TASK_NAME_PATTERN =
            Pattern.compile("[a-z]([-a-z0-9]*[a-z0-9])?");

    public static String getEffectiveTaskName(String taskName, String taskId) {
        if (StringUtils.isNotEmpty(taskName)) {
            taskName = StringUtils.lowerCase(taskName);
            taskName = StringUtils.splitByWholeSeparator(taskName, taskId)[0];
            taskName = taskName.replaceAll("\\p{P}", "-");
            taskName = String.format("%s%s", taskName, taskId);
            int taskNameLength = taskName.length();
            if (taskNameLength > 38) {
                taskName =
                        taskName.substring(
                                taskNameLength - 38,
                                taskNameLength);
            }
            Matcher matcher = EFFECTIVE_TASK_NAME_PATTERN.matcher(taskName);
            if (!matcher.matches()) {
                String tmpStr;
                taskNameLength = taskName.length();

                if (taskNameLength + 2 >= 38) {
                    taskName =
                            taskName.substring(
                                    taskNameLength - 38 + 2,
                                    taskNameLength);
                }
                tmpStr = "k-" + taskName;
                LOG.info("taskName:[{}] is invalid, new taskName is:[{}]", taskName, tmpStr);
                taskName = tmpStr;
            }
        }
        return taskName;
    }



    public static PackagedProgram buildProgram(
            String jarPath,
            List<URL> classPaths,
            String entryPointClass,
            String[] programArgs,
            SavepointRestoreSettings spSetting,
            Configuration flinkConfiguration)
            throws ProgramInvocationException {
        if (jarPath == null) {
            throw new IllegalArgumentException("The program JAR file was not specified.");
        }
        File jarFile = new File(jarPath);

        Configuration flinkConfig =
                new Configuration(flinkConfiguration);


        // 指定采用parent ClassLoader优先加载的类
//        String append =
//                flinkConfig.getString(CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL);
//        String dtstackAppend = "com.fasterxml.jackson.";
//        if (StringUtils.isNotEmpty(append)) {
//            dtstackAppend = dtstackAppend + ";" + append;
//        }
//        flinkConfig.setString(
//                CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL, dtstackAppend);

        PackagedProgram program =
                PackagedProgram.newBuilder()
                        .setJarFile(jarFile)
                        .setUserClassPaths(classPaths)
                        .setEntryPointClassName(entryPointClass)
                        .setConfiguration(flinkConfig)
                        .setArguments(programArgs)
                        .setSavepointRestoreSettings(spSetting)
                        .build();

        return program;
    }

    public static ClusterSpecification getClusterSpecification(Configuration configuration) {
        Preconditions.checkNotNull(configuration);

        final int jobManagerMemoryMB =
                JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(
                                configuration, JobManagerOptions.TOTAL_PROCESS_MEMORY)
                        .getTotalProcessMemorySize()
                        .getMebiBytes();

        final int taskManagerMemoryMB =
                TaskExecutorProcessUtils.processSpecFromConfig(
                                TaskExecutorProcessUtils
                                        .getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
                                                configuration,
                                                TaskManagerOptions.TOTAL_PROCESS_MEMORY))
                        .getTotalProcessMemorySize()
                        .getMebiBytes();

        int slotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);

        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobManagerMemoryMB)
                .setTaskManagerMemoryMB(taskManagerMemoryMB)
                .setSlotsPerTaskManager(slotsPerTaskManager)
                .createClusterSpecification();
    }


    public static RdosTaskStatus getPodStatus(KubernetesPod pod) {
        PodStatus status = pod.getInternalResource().getStatus();

        RdosTaskStatus taskStatus = null;
        String phase = status.getPhase();
        if (phase == null) {
            phase = "";
        } else {
            phase = phase.toLowerCase(Locale.ENGLISH);
        }
        switch (phase) {
            case "succeeded":
                taskStatus = FINISHED;
                break;
            case "failed":
                taskStatus = FAILED;
                break;
            case "unkonwn":
                taskStatus = NOTFOUND;
                break;
            case "pending":
                taskStatus = CREATED;
                break;
            default:
                // Phase = RUNNING
                boolean allRunning = true;
                boolean readying = false;
                boolean hasTerminated = false;
                boolean hasCreated = false;
                for (ContainerStatus containerStatus : status.getContainerStatuses()) {
                    Boolean ready = containerStatus.getReady();
                    ContainerState state = containerStatus.getState();
                    ContainerStateTerminated terminated = state.getTerminated();
                    ContainerStateWaiting waiting = state.getWaiting();
                    ContainerStateRunning running = state.getRunning();
                    if (!ready || terminated != null || waiting != null || running == null) {
                        allRunning = false;
                    }
                    if (running != null && !ready) {
                        readying = true;
                        LOG.error(
                                "-------imageName {}, container ID {} is running but not ready----",
                                containerStatus.getImage(),
                                containerStatus.getContainerID());
                    }
                    // pod停止中
                    if (terminated != null) {
                        hasTerminated = true;
                        String reason = terminated.getReason();
                        LOG.error(
                                "-------imageName {}, container ID {} hasTerminated,  reason is {} ----",
                                containerStatus.getImage(),
                                terminated.getContainerID(),
                                reason);
                    }
                    if (waiting != null) {
                        hasCreated = true;
                        LOG.error(
                                "-------imageName {}, container ID {} hasCreated, reason is {} ----",
                                containerStatus.getImage(),
                                containerStatus.getContainerID(),
                                //CrashLoopBackOff
                                waiting.getReason());
                    }
                }
                // running是每个容器都是running 且是ready状态
                if (allRunning) {
                    taskStatus = RdosTaskStatus.RUNNING;
                } else if (hasTerminated) {
                    taskStatus = RdosTaskStatus.CANCELLING;
                } else if (hasCreated) {
                    taskStatus = RdosTaskStatus.CREATED;
                } else if (readying) {
                    taskStatus = RdosTaskStatus.DEPLOYING;
                } else {
                    taskStatus = FAILED;
                }
                break;
        }
        return taskStatus;
    }
}
