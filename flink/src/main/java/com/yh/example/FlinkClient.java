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

package com.yh.example;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.concurrent.CompletableFuture;

public class FlinkClient {
    public static void main(String[] args) throws Exception {
        YarnConfiguration configuration = new YarnConfiguration();
        configuration.addResource("hadoop-node/core-site.xml");
        configuration.addResource("hadoop-node/hdfs-site.xml");
        configuration.addResource("hadoop-node/yarn-site.xml");

        String applicationId = "application_1681649148089_33393";

        final YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);
        yarnClient.start();

        org.apache.flink.configuration.Configuration flinkConfiguration = new org.apache.flink.configuration.Configuration();
        flinkConfiguration.setString("high-availability.zookeeper.quorum","172.16.83.214:2181,172.16.83.225:2181,172.16.83.73:2181");
        flinkConfiguration.setString("high-availability","ZOOKEEPER");
        flinkConfiguration.setString("high-availability.zookeeper.path.root","/flink112");

        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                flinkConfiguration,
                configuration,
                yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                false);

        ClusterClient<ApplicationId> clusterClient = null;

        ClusterClientProvider<ApplicationId> clusterClientProvider =
                yarnClusterDescriptor.retrieve(ConverterUtils.toApplicationId(applicationId));
        clusterClient = clusterClientProvider.getClusterClient();
//        ClusterClient<ApplicationId>  clusterClient1 = clusterClientProvider.getClusterClient();
//        ClusterClient<ApplicationId>  clusterClient2= clusterClientProvider.getClusterClient();
        final ApplicationReport report = yarnClient.getApplicationReport(ConverterUtils.toApplicationId(applicationId));

        YarnApplicationState yarnApplicationState = report.getYarnApplicationState();
        if (report.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
            // Flink cluster is not running anymore
            throw new RuntimeException(
                    "The Yarn application " + applicationId + " doesn't run anymore.");
        }

        String webInterfaceURL = clusterClient.getWebInterfaceURL();
        Thread.sleep(600000L);

    }
}
