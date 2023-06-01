package org.example;

import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.executors.KubernetesSessionClusterExecutor;

public class Main {
    public static void main(String[] args)throws Exception {
        Configuration flinkConfigure = GlobalConfiguration.loadConfiguration("/Users/yh/Downloads/flink-1.12.7/conf");
        flinkConfigure.setString("kubernetes.cluster-id","flink-k8s-session-cluster-yh");
        flinkConfigure.setString("kubernetes.jobmanager.service-account","flink");
        flinkConfigure.setString("kubernetes.rest-service.exposed.type","NodePort");
        flinkConfigure.setString("taskmanager.numberOfTaskSlots","2");
        flinkConfigure.setString("taskmanager.memory.process.size","2g");
        flinkConfigure.setString("kubernetes.taskmanager.cpu","1");
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
        System.out.println("Hello world!");
    }
}