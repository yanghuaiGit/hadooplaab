package org.example;

import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.ResourceQuotaList;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.*;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.executors.KubernetesSessionClusterExecutor;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Main  {
    public static void main(String[] args) throws Exception {
        String conf = "/Users/yh/Downloads/flink-1.12.7/conf";

        Configuration flinkConfigure = GlobalConfiguration.loadConfiguration(conf);
        flinkConfigure.setString("kubernetes.cluster-id", "flink-k8s-session-cluster-yh");
        flinkConfigure.setString("kubernetes.jobmanager.service-account", "flink");
        flinkConfigure.setString("kubernetes.namespace", "dujie");
        flinkConfigure.setString("kubernetes.rest-service.exposed.type", "NodePort");
        flinkConfigure.setString("taskmanager.numberOfTaskSlots", "2");
        flinkConfigure.setString("taskmanager.memory.process.size", "2g");
        flinkConfigure.setString("kubernetes.taskmanager.cpu", "1");
        flinkConfigure.setString("kubernetes.container.image", "172.16.84.106/dtstack-dev/flinkk8s:1.0");
        flinkConfigure.setString("kubernetes.container.image.pull-policy", "Always");
        flinkConfigure.setString("classloader.resolve-order", "parent-first");
        flinkConfigure.setString("kubernetes.service-account", "flink");

        FlinkKubeClient client = FlinkKubeClientFactory.getInstance()
                .fromConfiguration(flinkConfigure, "client");


        Map<String, String> labelsMap = new HashMap<>();

        labelsMap.put("component", "jobmanager");
        labelsMap.put(Constants.LABEL_APP_KEY, "dujie-hivecatalog-4m0tmnd1aan0-meby");
        labelsMap.put(Constants.LABEL_TASK_ID_KEY, "4m0tmnd1aan0");
        List<KubernetesPod> ps = client.getPodsWithLabels(labelsMap);
        KubernetesPluginUtil.getPodStatus(ps.get(0));

//        Boolean deleted =  getKubernetClient(client).pods()
//                .inNamespace("default")  // pod所在的命名空间
//                .withName("pod-name")    // 要删除的pod的名称
//                .withGracePeriod(0)      // 设置宽限期为0以实现强制删除
//                .delete();


        ResourceQuotaList resourceQuotas =
                getKubernetClient(client).resourceQuotas().inNamespace("dujie").list();


        session(flinkConfigure);
//        application(flinkConfigure);
    }

    public static NamespacedKubernetesClient getKubernetClient(FlinkKubeClient flinkKubeClient) {
        try {
            Field internalClientField =
                    Fabric8FlinkKubeClient.class.getDeclaredField("internalClient");
            internalClientField.setAccessible(true);
            return (NamespacedKubernetesClient) internalClientField.get(flinkKubeClient);

        } catch (Exception e) {
            throw new RuntimeException("get getKubernetClient failed", e);
        }
    }

    private static void application(Configuration flinkConfigure) throws Exception {

        String salt = RandomStringUtils.randomAlphanumeric(4).toLowerCase();
        String clusterId = String.format("%s-%s", "test", salt);
        flinkConfigure.set(KubernetesConfigOptions.CLUSTER_ID, clusterId);

        flinkConfigure.set(
                DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());


        String remoteCoreJarPath =
                "local://"
                        + "/opt/flink"
                        + File.separator
                        + "usrlib/chunjun-core-release_1.12_5.3.x.jar";
        flinkConfigure.set(
                PipelineOptions.JARS, Collections.singletonList(remoteCoreJarPath));

        // jobId, application模式jobGraph在远端生成，因此这里指定jobId
        flinkConfigure.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, JobID.generate().toString());


        // filebeat
        flinkConfigure.setString(
                ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX + "TASK_ID",
                clusterId);
        flinkConfigure.setString(
                ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX
                        + "TASK_ID",
                clusterId);

        ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(
                        buildFlinkxProgramArgs("kubernetes-application").toArray(new String[0]),
                        "com.dtstack.chunjun.Main");

        KubernetesClusterClientFactory kubernetesClusterClientFactory =
                new KubernetesClusterClientFactory();
        KubernetesClusterDescriptor descriptor =
                kubernetesClusterClientFactory.createClusterDescriptor(flinkConfigure);
        ClusterSpecification clusterSpecification =
                KubernetesPluginUtil.getClusterSpecification(flinkConfigure);
        ClusterClientProvider<String> clientProvider =
                descriptor.deployApplicationCluster(clusterSpecification, applicationConfiguration);
    }

    public static void session(Configuration flinkConfigure) throws Exception {
        flinkConfigure.set(DeploymentOptions.TARGET, KubernetesSessionClusterExecutor.NAME);

        final ClusterClientFactory<String> kubernetesClusterClientFactory =
                new KubernetesClusterClientFactory();
        ClusterDescriptor<String> kubernetesClusterDescriptor =
                kubernetesClusterClientFactory.createClusterDescriptor(flinkConfigure);


//        ClusterClient<String> clusterClient = kubernetesClusterDescriptor.retrieve("flink-k8s-session-cluster-yh").getClusterClient();


        ClusterClient<String> clusterClient = kubernetesClusterDescriptor
                .deploySessionCluster(
                        kubernetesClusterClientFactory.getClusterSpecification(
                                flinkConfigure))
                .getClusterClient();
        String clusterId = clusterClient.getClusterId();

        JobGraph jobGraph = buildJobgraph(flinkConfigure, "kubernetes-session");
        try {
            clusterClient.submitJob(jobGraph)
                    .thenApply(DetachedJobExecutionResult::new)
                    .get(30, TimeUnit.MINUTES);
        } catch (Exception e) {
            System.out.println(e);
        }

        System.out.println("Hello world!");
    }

    public static JobGraph buildJobgraph(Configuration flinkConfigure, String mode) throws Exception {
        List<String> programArgList = buildFlinkxProgramArgs(mode);

        PackagedProgram packagedProgram =
                KubernetesPluginUtil.buildProgram(
                        "/Users/yh/Code/workspace/flinkx_1.12/chunjun-dist/chunjun-core-release_1.12_5.3.x.jar",//chunjuncore路径
                        Lists.newArrayList(new File("/Users/yh/Code/workspace/flinkx_1.12/chunjun-dist/chunjun-common-release_1.12_5.3.x.jar").toURI().toURL()),//
                        "com.dtstack.chunjun.Main",
                        programArgList.toArray(new String[programArgList.size()]),
                        SavepointRestoreSettings.none(),
                        flinkConfigure);

        return
                PackagedProgramUtils.createJobGraph(
                        packagedProgram,
                        flinkConfigure,
                        1,
                        false);

    }

    public static List<String> buildFlinkxProgramArgs(String mode)
            throws Exception {

        List<String> programArgList = new ArrayList<>(16);
        //        if (StringUtils.isNotBlank(args)) {
        //            // 按空格,制表符等进行拆分
        //            programArgList.addAll(Arrays.asList(args.split("\\s+")));
        //        }
        programArgList.add("-mode");
        programArgList.add(mode);
        programArgList.add("-jobType");
        programArgList.add("sync");
        programArgList.add("-job");

        programArgList.add(getsync());
        programArgList.add("-jobName");
        programArgList.add("test");
        programArgList.add("-pluginLoadMode");
        programArgList.add("shipfile");
        programArgList.add("-flinkxDistDir");
        programArgList.add("/Users/yh/Code/workspace/flinkx_1.12/chunjun-dist");


        return programArgList;
    }

    public static String getsync() {
        return URLEncoder.encode("{\n" +
                "  \"job\": {\n" +
                "    \"content\": [\n" +
                "      {\n" +
                "        \"reader\": {\n" +
                "          \"parameter\": {\n" +
                "            \"column\": [\n" +
                "              {\n" +
                "                \"name\": \"id\",\n" +
                "                \"type\": \"string\"\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\": \"name\",\n" +
                "                \"type\": \"string\"\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\": \"content\",\n" +
                "                \"type\": \"string\"\n" +
                "              }\n" +
                "            ],\n" +
                "            \"sliceRecordCount\": [\"30\"],\n" +
                "            \"permitsPerSecond\": 1\n" +
                "          },\n" +
                "          \"table\": {\n" +
                "            \"tableName\": \"sourceTable\"\n" +
                "          },\n" +
                "          \"name\": \"streamreader\"\n" +
                "        },\n" +
                "        \"writer\": {\n" +
                "          \"parameter\": {\n" +
                "            \"column\": [\n" +
                "              {\n" +
                "                \"name\": \"id\",\n" +
                "                \"type\": \"string\"\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\": \"name\",\n" +
                "                \"type\": \"string\"\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\": \"content\",\n" +
                "                \"type\": \"string\"\n" +
                "              }\n" +
                "            ],\n" +
                "            \"print\": true\n" +
                "          },\n" +
                "          \"table\": {\n" +
                "            \"tableName\": \"sinkTable\"\n" +
                "          },\n" +
                "          \"name\": \"streamwriter\"\n" +
                "        }\n" +
                "      }\n" +
                "    ],\n" +
                "    \"setting\": {\n" +
                "      \"errorLimit\": {\n" +
                "        \"record\": 100\n" +
                "      },\n" +
                "      \"speed\": {\n" +
                "        \"bytes\": 0,\n" +
                "        \"channel\": 1,\n" +
                "        \"readerChannel\": 1,\n" +
                "        \"writerChannel\": 1\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n");
    }

    public static void test() throws Exception {
        String job = "{\n" +
                "  \"job\": {\n" +
                "    \"content\": [\n" +
                "      {\n" +
                "        \"reader\": {\n" +
                "          \"parameter\": {\n" +
                "            \"column\": [\n" +
                "              {\n" +
                "                \"name\": \"id\",\n" +
                "                \"type\": \"string\"\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\": \"name\",\n" +
                "                \"type\": \"string\"\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\": \"content\",\n" +
                "                \"type\": \"string\"\n" +
                "              }\n" +
                "            ],\n" +
                "            \"sliceRecordCount\": [\"30\"],\n" +
                "            \"permitsPerSecond\": 1\n" +
                "          },\n" +
                "          \"table\": {\n" +
                "            \"tableName\": \"sourceTable\"\n" +
                "          },\n" +
                "          \"name\": \"streamreader\"\n" +
                "        },\n" +
                "        \"writer\": {\n" +
                "          \"parameter\": {\n" +
                "            \"column\": [\n" +
                "              {\n" +
                "                \"name\": \"id\",\n" +
                "                \"type\": \"string\"\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\": \"name\",\n" +
                "                \"type\": \"string\"\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\": \"content\",\n" +
                "                \"type\": \"string\"\n" +
                "              }\n" +
                "            ],\n" +
                "            \"print\": true\n" +
                "          },\n" +
                "          \"name\": \"streamwriter\"\n" +
                "        }\n" +
                "      }\n" +
                "    ],\n" +
                "    \"setting\": {\n" +
                "      \"errorLimit\": {\n" +
                "        \"record\": 100\n" +
                "      },\n" +
                "      \"speed\": {\n" +
                "        \"bytes\": 0,\n" +
                "        \"channel\": 1,\n" +
                "        \"readerChannel\": 1,\n" +
                "        \"writerChannel\": 1\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        String decode = URLEncoder.encode(job);
        String a = "-p,,-job," + decode + ",-jobName,Flink_Job,-flinkxDistDir,/Users/yh/Code/workspace/flinkx_1.12/chunjun-dist,-jobType,sync,-hadoopConfDir,/Users/yh/Downloads/238_conf,-confProp,{\n"
                + "  \"classloader.parent-first-patterns.additional\": \"com.fasterxml.jackson.;com.dtstack.flinkx.util.FactoryHelper;com.dtstack.chunjun.util.FactoryHelper;org.codehaus.\"\n"
                + "},-pluginLoadMode,shipfile,-mode,yarn-session,-flinkConfDir,/Users/yh/Code/environment/1.12/flinkx2/conf";
        String[] split = a.split(",");

        Configuration configuration = GlobalConfiguration.loadConfiguration("/Users/yh/Code/environment/1.12/flinkx2/conf");
        configuration.setString("pluginLoadMode", "shipfile");

        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);
        configuration.set(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
        // 指定采用parent ClassLoader优先加载的类
//        String append =
//                configuration.getString(CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL);
//        String dtstackAppend = "com.fasterxml.jackson.";
//        if (StringUtils.isNotEmpty(append)) {
//            dtstackAppend = dtstackAppend + ";" + append;
//        }
//        dtstackAppend = dtstackAppend + ";" + "org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders";
//        configuration.setString(
//                CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL, dtstackAppend);

        PackagedProgram packagedProgram =
                PackagedProgram.newBuilder()
                        .setJarFile(new File("/Users/yh/Code/workspace/flinkx_1.12/chunjun-dist/chunjun-core-release_1.12_5.3.x.jar"))
                        .setUserClassPaths(Lists.newArrayList(new File("/Users/yh/Code/workspace/flinkx_1.12/chunjun-dist/chunjun-common-release_1.12_5.3.x.jar").toURI().toURL()))
                        .setEntryPointClassName("com.dtstack.chunjun.Main")
                        .setConfiguration(configuration)
                        .setArguments(split)
                        .build();


        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(
                packagedProgram,
                configuration,
                1,
                false);
        System.out.println(jobGraph.getUserJars());
    }
}