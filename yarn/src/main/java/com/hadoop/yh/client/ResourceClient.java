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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.ArrayList;
import java.util.List;

public class ResourceClient {
    public static void main(String[] args) throws Exception {
        int containerCoreMax = 0;
        int containerMemoryMax = 0;
        Configuration configuration = new YarnConfiguration();
        configuration.addResource("op/core-site.xml");
        configuration.addResource("op/hdfs-site.xml");
        configuration.addResource("op/yarn-site.xml");
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);
        yarnClient.start();

        List<NodeReport> nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);

        ArrayList<NodeResourceDetail> nodeResourceDetails = new ArrayList<>();
        for (NodeReport report : nodeReports) {
            Resource capability = report.getCapability();
            Resource used = report.getUsed();
            int totalMem = capability.getMemory();
            int totalCores = capability.getVirtualCores();

            int usedMem = used.getMemory();
            int usedCores = used.getVirtualCores();

            int freeCores = totalCores - usedCores;
            int freeMem = totalMem - usedMem;

            if (freeCores > containerCoreMax) {
                containerCoreMax = freeCores;
            }
            if (freeMem > containerMemoryMax) {
                containerMemoryMax = freeMem;
            }

            NodeResourceDetail nodeResourceDetail = new NodeResourceDetail(
                    report.getNodeId().toString(),
                    totalCores,
                    usedCores,
                    freeCores,
                    totalMem,
                    usedMem,
                    freeMem);

            nodeResourceDetails.add(nodeResourceDetail);

        }
        System.out.println(nodeResourceDetails);


    }


    public static class NodeResourceDetail {
        private String nodeId;
        private int coresTotal;
        private int coresUsed;
        private int coresFree;
        private int memoryTotal;
        private int memoryUsed;
        private int memoryFree;

        public NodeResourceDetail(
                String nodeId,
                int coresTotal,
                int coresUsed,
                int coresFree,
                int memoryTotal,
                int memoryUsed,
                int memoryFree) {
            this.nodeId = nodeId;
            this.coresTotal = coresTotal;
            this.coresUsed = coresUsed;
            this.coresFree = coresFree;
            this.memoryTotal = memoryTotal;
            this.memoryUsed = memoryUsed;
            this.memoryFree = memoryFree;
        }

        @Override
        public String toString() {
            return "NodeResourceDetail{" +
                    "nodeId='" + nodeId + '\'' +
                    ", coresTotal=" + coresTotal +
                    ", coresUsed=" + coresUsed +
                    ", coresFree=" + coresFree +
                    ", memoryTotal=" + memoryTotal +
                    ", memoryUsed=" + memoryUsed +
                    ", memoryFree=" + memoryFree +
                    '}';
        }
    }
}
