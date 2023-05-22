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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hadoop.yh.util.StatsUtils;
import org.apache.commons.collections.MapUtils;
import org.example.util.HttpUtil;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TaskManagerClient {
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();


    public static void main(String[] args) throws Exception {
        printTaskmanagerInfo();
    }

    public static void printTaskmanagerInfo() throws Exception {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "哈哈哈哈"));
        CopyOnWriteArrayList<Date> timestamps = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Integer> values = new CopyOnWriteArrayList<>();
        //todo 每隔5s查询下客户的任务
        long l = System.currentTimeMillis();
        executor.scheduleAtFixedRate(() -> {
            String jobCount = "http://hadoop-node3:34981/jobs";
            try {
                runningJobCount(jobCount, timestamps, values);
            } catch (Exception e) {
            }

        }, 0, 5, TimeUnit.SECONDS);

        while (true) {
            if (System.currentTimeMillis() - l > 10 * 60 * 1000) {
                break;
            }
        }

        int min = StatsUtils.getMin(values);
        int max = StatsUtils.getMax(values);
        double median = StatsUtils.getMedian(values);
        double mean = StatsUtils.getMean(values);
        System.out.println(String.format("最小值 %s, 最大值 %s, 中位数 %s, 中间值 %s ", min, max, median, mean));
        System.out.println(OBJ_MAPPER.writeValueAsString(values));
        String url = "http://op3:33468/taskmanagers";
        List<Double> collect = values.stream().map(Integer::doubleValue).collect(Collectors.toList());
        Print.plotTimeSeries(timestamps, collect, "Time Series Chart", "时间戳", "运行任务数量");
    }

    public static void runningJobCount(String url,CopyOnWriteArrayList<Date> timestamps,CopyOnWriteArrayList<Integer> values) throws Exception {
        Date date = new Date();
        int count = 0;
        String message = HttpUtil.get(url);
        if (message != null) {
            Map<String, Object> jobInfos = OBJ_MAPPER.readValue(message, Map.class);
            if (jobInfos.containsKey("jobs")) {
                List<Map<String, Object>> jobList =
                        (List<Map<String, Object>>) jobInfos.get("jobs");
                if (jobList.size() == 0) {
                    count = 0;
                } else {
                    int i = 0;
                    for (Map<String, Object> tmp : jobList) {
                        String id = MapUtils.getString(tmp, "id");
                        String status = MapUtils.getString(tmp, "status");
                        //RUNNING CANCELLING  CANCELED FINISHED
                        if (status.equals("RUNNING")) {
                            i++;
                        }
                    }
                    count = i;
                }
            }else{
                count = 0;
            }
        }
        timestamps.add(date);
        values.add(count);
    }

    public static String taskManagerInfo(String url) throws Exception {
        String message = HttpUtil.get(url);
        if (message != null) {
            Map<String, Object> taskManagerInfo = OBJ_MAPPER.readValue(message, Map.class);
            if (taskManagerInfo.containsKey("taskmanagers")) {
                List<Map<String, Object>> taskManagerList =
                        (List<Map<String, Object>>) taskManagerInfo.get("taskmanagers");
                if (taskManagerList.size() == 0) {
                    System.out.println("session没有任何taskManager");
                } else {
                    int totalUsedSlots = 0;
                    int totalFreeSlots = 0;
                    int totalSlotsNumber = 0;
                    for (Map<String, Object> tmp : taskManagerList) {
                        int freeSlots = MapUtils.getIntValue(tmp, "freeSlots");
                        int slotsNumber = MapUtils.getIntValue(tmp, "slotsNumber");
                        totalUsedSlots += slotsNumber - freeSlots;
                        totalFreeSlots += freeSlots;
                        totalSlotsNumber += slotsNumber;
                    }

                    System.out.println(String.format("totalSlotsNumber:%s,totalFreeSlots:%s, totalUsedSlots:%s",
                            totalSlotsNumber,
                            totalFreeSlots,
                            totalUsedSlots));
                }
            }
        }
        return null;
    }

}
