package org.example;

import com.google.common.collect.Lists;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.executors.KubernetesSessionClusterExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration flinkConfigure = GlobalConfiguration.loadConfiguration("/Users/yh/Downloads/flink-1.12.7/conf");
        flinkConfigure.setString("kubernetes.cluster-id", "flink-k8s-session-cluster-yh");
        flinkConfigure.setString("kubernetes.jobmanager.service-account", "flink");
        flinkConfigure.setString("kubernetes.rest-service.exposed.type", "NodePort");
        flinkConfigure.setString("taskmanager.numberOfTaskSlots", "2");
        flinkConfigure.setString("taskmanager.memory.process.size", "2g");
        flinkConfigure.setString("kubernetes.taskmanager.cpu", "1");
        flinkConfigure.set(DeploymentOptions.TARGET, KubernetesSessionClusterExecutor.NAME);

        final ClusterClientFactory<String> kubernetesClusterClientFactory =
                new KubernetesClusterClientFactory();
        ClusterDescriptor<String> kubernetesClusterDescriptor =
                kubernetesClusterClientFactory.createClusterDescriptor(flinkConfigure);

        ClusterClient<String> clusterClient = kubernetesClusterDescriptor
                .deploySessionCluster(
                        kubernetesClusterClientFactory.getClusterSpecification(
                                flinkConfigure))
                .getClusterClient();
        String clusterId = clusterClient.getClusterId();


        clusterClient.submitJob(buildJobgraph(flinkConfigure))
                .thenApply(DetachedJobExecutionResult::new)
                .get(5, TimeUnit.MINUTES);

        System.out.println("Hello world!");
    }

    public static JobGraph buildJobgraph(Configuration flinkConfigure) throws Exception {
        List<String> programArgList = buildFlinkxProgramArgs();

        PackagedProgram packagedProgram =
                KubernetesPluginUtil.buildProgram(
                        "/opt/data/chunjun-dist/chunjun-core-release_1.12_5.3.x.jar",//chunjuncore路径
                        Lists.newArrayList(),//
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

    public static List<String> buildFlinkxProgramArgs()
            throws Exception {

        List<String> programArgList = new ArrayList<>(16);
        //        if (StringUtils.isNotBlank(args)) {
        //            // 按空格,制表符等进行拆分
        //            programArgList.addAll(Arrays.asList(args.split("\\s+")));
        //        }
        programArgList.add("-mode");
        String mode = "kubernetes-session";
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
        programArgList.add("/opt/data/chunjun-dist");


        return programArgList;
    }

    public static String getsync() {
        return "{\n" +
                "  \"job\": {\n" +
                "    \"content\": [\n" +
                "      {\n" +
                "        \"reader\": {\n" +
                "          \"parameter\": {\n" +
                "            \"column\": [\n" +
                "              {\n" +
                "                \"name\": \"id\",\n" +
                "                \"type\": \"id\"\n" +
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
                "                \"type\": \"id\"\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\": \"name\",\n" +
                "                \"type\": \"string\"\n" +
                "              },\n" +
                "              {\n" +
                "                \"name\": \"content\",\n" +
                "                \"type\": \"timestamp\"\n" +
                "              }\n" +
                "            ],\n" +
                "            \"print\": true\n" +
                "          },\n" +
                "          \"table\": {\n" +
                "            \"tableName\": \"sinkTable\"\n" +
                "          },\n" +
                "          \"name\": \"streamwriter\"\n" +
                "        },\n" +
                "        \"transformer\": {\n" +
                "          \"transformSql\": \"select id,name, NOW() from sourceTable where CHAR_LENGTH(name) < 50 and CHAR_LENGTH(content) < 50\"\n" +
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
    }

}