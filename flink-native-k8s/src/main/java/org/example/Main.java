package org.example;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.executors.KubernetesSessionClusterExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
//        test();
        Configuration flinkConfigure = GlobalConfiguration.loadConfiguration("/Users/yh/Downloads/flink-1.12.7/conf");
        flinkConfigure.setString("kubernetes.cluster-id", "flink-k8s-session-cluster-yh");
        flinkConfigure.setString("kubernetes.jobmanager.service-account", "flink");
        flinkConfigure.setString("kubernetes.rest-service.exposed.type", "NodePort");
        flinkConfigure.setString("taskmanager.numberOfTaskSlots", "2");
        flinkConfigure.setString("taskmanager.memory.process.size", "2g");
        flinkConfigure.setString("kubernetes.taskmanager.cpu", "1");
        flinkConfigure.setString("kubernetes.container.image", "yhbest/flinkk8s:1.2");

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
        programArgList.add("/Users/yh/Code/workspace/flinkx_1.12/chunjun-dist");


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

    public static void test() throws Exception {
        String a = "-p,,-job,%7B%0A++%22job%22%3A+%7B%0A++++%22content%22%3A+%5B%0A++++++%7B%0A++++++++%22reader%22%3A+%7B%0A++++++++++%22parameter%22%3A+%7B%0A++++++++++++%22column%22%3A+%5B%0A++++++++++++++%7B%0A++++++++++++++++%22name%22%3A+%22id%22%2C%0A++++++++++++++++%22type%22%3A+%22string%22%0A++++++++++++++%7D%2C%0A++++++++++++++%7B%0A++++++++++++++++%22name%22%3A+%22name%22%2C%0A++++++++++++++++%22type%22%3A+%22string%22%0A++++++++++++++%7D%2C%0A++++++++++++++%7B%0A++++++++++++++++%22name%22%3A+%22content%22%2C%0A++++++++++++++++%22type%22%3A+%22string%22%0A++++++++++++++%7D%0A++++++++++++%5D%2C%0A++++++++++++%22sliceRecordCount%22%3A+%5B%2230%22%5D%2C%0A++++++++++++%22permitsPerSecond%22%3A+1%0A++++++++++%7D%2C%0A++++++++++%22table%22%3A+%7B%0A++++++++++++%22tableName%22%3A+%22sourceTable%22%0A++++++++++%7D%2C%0A++++++++++%22name%22%3A+%22streamreader%22%0A++++++++%7D%2C%0A++++++++%22writer%22%3A+%7B%0A++++++++++%22name%22%3A+%22kafkasink%22%2C%0A++++++++++%22parameter%22%3A+%7B%0A++++++++++++%22tableFields%22%3A+%5B%0A++++++++++++++%22id%22%2C%0A++++++++++++++%22name%22%2C%0A++++++++++++++%22content%22%0A++++++++++++%5D%2C%0A++++++++++++%22topic%22%3A+%22e2e_json%22%2C%0A++++++++++++%22producerSettings%22%3A+%7B%0A++++++++++++++%22auto.commit.enable%22%3A+%22false%22%2C%0A++++++++++++++%22bootstrap.servers%22%3A+%22172.16.100.109%3A9092%22%0A++++++++++++%7D%0A++++++++++%7D%0A++++++++%7D%0A++++++%7D%0A++++%5D%2C%0A++++%22setting%22%3A+%7B%0A++++++%22restore%22%3A+%7B%0A++++++++%22isRestore%22%3A+true%2C%0A++++++++%22isStream%22%3A+true%0A++++++%7D%2C%0A++++++%22errorLimit%22%3A+%7B%0A++++++++%22record%22%3A+0%0A++++++%7D%2C%0A++++++%22speed%22%3A+%7B%0A++++++++%22readerChannel%22%3A+1%2C%0A++++++++%22writerChannel%22%3A+1%0A++++++%7D%0A++++%7D%0A++%7D%0A%7D%0A,-jobName,Flink_Job,-flinkxDistDir,/Users/yh/Code/workspace/flinkx_1.12/chunjun-dist,-jobType,sync,-hadoopConfDir,/Users/yh/Downloads/238_conf,-confProp,{\n"
                + "  \"classloader.parent-first-patterns.additional\": \"com.fasterxml.jackson.;com.dtstack.flinkx.util.FactoryHelper;com.dtstack.chunjun.util.FactoryHelper;org.codehaus.\"\n"
                + "},-pluginLoadMode,shipfile,-mode,yarn-session,-flinkConfDir,/Users/yh/Code/environment/1.12/flinkx2/conf";
        String[] split = a.split(",");

        Configuration configuration = GlobalConfiguration.loadConfiguration("/Users/yh/Code/environment/1.12/flinkx2/conf");
        configuration.setString("pluginLoadMode", "shipfile");

        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);
        configuration.set(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
        // 指定采用parent ClassLoader优先加载的类
        String append =
                configuration.getString(CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL);
        String dtstackAppend = "com.fasterxml.jackson.";
        if (StringUtils.isNotEmpty(append)) {
            dtstackAppend = dtstackAppend + ";" + append;
        }
        dtstackAppend = dtstackAppend+";"+"org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders";
        configuration.setString(
                CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL, dtstackAppend);

        PackagedProgram packagedProgram =
                PackagedProgram.newBuilder()
                        .setJarFile(new File("/Users/yh/Code/workspace/flinkx_1.12/chunjun-dist/chunjun-core-release_1.12_5.3.x.jar"))
                        .setUserClassPaths(Lists.newArrayList(new File("/Users/yh/Code/workspace/flinkx_1.12/chunjun-dist/chunjun-common-release_1.12_5.3.x.jar").toURI().toURL()))
                        .setEntryPointClassName("com.dtstack.chunjun.Main")
                        .setConfiguration(configuration)
                        .setArguments(split)
                        .build();


        PackagedProgramUtils.createJobGraph(
                packagedProgram,
                configuration,
                1,
                false);
    }
}