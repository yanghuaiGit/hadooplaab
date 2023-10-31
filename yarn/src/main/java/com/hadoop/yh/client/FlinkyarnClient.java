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

package com.hadoop.yh.client;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public class FlinkyarnClient {
    public static void main(String[] args) throws Exception {
        YarnConfiguration configuration = new YarnConfiguration();
        configuration.addResource("hadoop-node/core-site.xml");
        configuration.addResource("hadoop-node/hdfs-site.xml");
        configuration.addResource("hadoop-node/yarn-site.xml");

        String applicationId = "stream_ncsi_stream_hive";

        final YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);
        yarnClient.start();

        while (true) {
            killApplication(yarnClient, applicationId);
            Thread.sleep(10000L);
        }

//        org.apache.flink.configuration.Configuration flinkConfiguration = new org.apache.flink.configuration.Configuration();
//        flinkConfiguration.setString("high-availability.zookeeper.quorum","172.16.83.214:2181,172.16.83.225:2181,172.16.83.73:2181");
//        flinkConfiguration.setString("high-availability","ZOOKEEPER");
//        flinkConfiguration.setString("high-availability.zookeeper.path.root","/flink112");
//
//        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
//                flinkConfiguration,
//                configuration,
//                yarnClient,
//                YarnClientYarnClusterInformationRetriever.create(yarnClient),
//                false);
//
//
//        ClusterClientProvider<ApplicationId> clusterClientProvider =
//                yarnClusterDescriptor.retrieve(ConverterUtils.toApplicationId(applicationId));
//        ClusterClient<ApplicationId> clusterClient = clusterClientProvider.getClusterClient();
//
//        final ApplicationReport report = yarnClient.getApplicationReport(ConverterUtils.toApplicationId(applicationId));
//
//        YarnApplicationState yarnApplicationState = report.getYarnApplicationState();
//        if (report.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
//            // Flink cluster is not running anymore
//            throw new RuntimeException(
//                    "The Yarn application " + applicationId + " doesn't run anymore.");
//        }
//
//        String webInterfaceURL = clusterClient.getWebInterfaceURL();
//        Thread.sleep(600000L);

    }

    public static void killApplication(YarnClient yarnClient, String applicationId) throws Exception {
        Set<String> set = new HashSet<>();
        set.add("Apache Flink");
        set.add("Apache SPARK");
        set.add("SPARK");
        EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
        enumSet.add(YarnApplicationState.RUNNING);
        enumSet.add(YarnApplicationState.ACCEPTED);

        for (ApplicationReport application : yarnClient.getApplications(set, enumSet)) {
//            if (!application.getApplicationId().toString().equals(applicationId)) {
//                yarnClient.killApplication(application.getApplicationId());
//            }

            if(!application.getName().toString().startsWith(applicationId)){
                yarnClient.killApplication(application.getApplicationId());
            }
        }
    }
}
