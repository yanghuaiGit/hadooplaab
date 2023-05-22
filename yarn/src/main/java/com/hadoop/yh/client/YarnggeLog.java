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

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class YarnggeLog {


    //org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader.getContainerLogsReader
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.addResource("172-16-23-232/core-site.xml");
        configuration.addResource("172-16-23-232/hdfs-site.xml");
        configuration.addResource("172-16-23-232/yarn-site.xml");

        String applicationId = "application_1683627677053_1663";
        String jobId = "f2c17817ccc00543e92abc3be86b6111";

        String logPath = "hdfs://ns1/tmp/logs/admin/logs/";

        FileSystem fileSystem = FileSystem.get(configuration);
        RemoteIterator<LocatedFileStatus> dirs = fileSystem.listFiles(new Path(logPath + applicationId), false);

        List<String>taskManagerCOntainerIds = taskManagerContainerId(jobId,configuration);
        if(CollectionUtils.isEmpty(taskManagerCOntainerIds)){
            return;
        }
        //对每个NodeManager的日志进行循环查询
        while (dirs.hasNext()) {
            Path path = dirs.next().getPath();
            List<String> jobamnagerLog = new ArrayList<>();
            List<String> taskmanagerLog = new ArrayList<>();
            test(configuration, path, jobamnagerLog, taskmanagerLog,jobId,taskManagerCOntainerIds);
            System.out.println("success");
        }


    }

    public static List<String> taskManagerContainerId(String jobid,Configuration configuration)throws Exception{
        Set<String> data = new HashSet<>();
        // 控制台 flink配置 jobmanager.archive.fs.dir 对应的路径
        String archiveDir = "hdfs://ns1/dtInsight/flink112/completed-jobs";
        String jobArchivePath = archiveDir + "/" + jobid;
        JsonParser jsonParser = new JsonParser();
        FileSystem fileSystem = FileSystem.get(configuration);
        try (InputStream is =
                     fileSystem.open(new Path(jobArchivePath));
             InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
            JsonObject jobArchiveAll = (JsonObject) jsonParser.parse(reader);
            Preconditions.checkNotNull(jobArchiveAll, jobid + "jobArchive is null");
            JsonArray jsonArray = jobArchiveAll.getAsJsonArray("archive");
            List<String> verticesUrls = new ArrayList<>();
            for (JsonElement ele : jsonArray) {
                JsonObject obj = ele.getAsJsonObject();
                /// get job inforMation from jobs/jobId
                if (StringUtils.equals(
                        obj.get("path").getAsString(),
                        String.format( "/jobs/%s", jobid))) {
                    String exception = obj.get("json").getAsString();
                    if (StringUtils.isNotBlank(exception)) {
                        JsonElement element = jsonParser.parse(exception);
                        if (!element.isJsonObject()) {
                            continue;
                        }
                        JsonObject parse = (JsonObject) element;
                        for (JsonElement vertices : parse.getAsJsonArray("vertices")) {
                            JsonObject asJsonObject = vertices.getAsJsonObject();
                            verticesUrls.add(
                                    String.format(
                                            "/jobs/%s/vertices/%s",
                                            jobid,
                                            asJsonObject.get("id").toString().substring(1, asJsonObject.get("id").toString().length() - 1)));
                        }
                    }
                }
            }
            // 没有就返回空
            if (verticesUrls.size() == 0) {
                return Collections.EMPTY_LIST;
            }
            // pase data like
            // {
            // "path":"/jobs/5f14320f061bf3501b647760648cb3e8/vertices/bc764cd8ddf7a0cff126f51c16239658",
            //            "json":"{\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"name\":\"Source:
            // streamsourcefactory\",\"parallelism\":1,\"now\":1679239165429,\"subtasks\":[{\"subtask\":0,\"status\":\"FINISHED\",\"attempt\":0,\"host\":\"172-16-23-232\",\"start-time\":1679239024924,\"end-time\":1679239145412,\"duration\":120488,\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":true,\"write-bytes\":3313,\"write-bytes-complete\":true,\"read-records\":0,\"read-records-complete\":true,\"write-records\":100,\"write-records-complete\":true},\"taskmanager-id\":\"container_e20_1677137002310_366101_01_000013\",\"start_time\":1679239024924}]}"
            //        }

            for (JsonElement ele : jsonArray) {
                JsonObject obj = ele.getAsJsonObject();
                if (verticesUrls.contains(obj.get("path").getAsString())) {
                    String exception = obj.get("json").getAsString();
                    if (StringUtils.isNotBlank(exception)) {
                        JsonElement element = jsonParser.parse(exception);
                        if (!element.isJsonObject()) {
                            continue;
                        }
                        JsonObject parse = (JsonObject) element;
                        for (JsonElement subtasks : parse.get("subtasks").getAsJsonArray()) {
                            JsonObject subTask = subtasks.getAsJsonObject();
                            String containerId = subTask.get("taskmanager-id").getAsString();
                            data.add(containerId);
                        }
                    }
                }
            }
        }
        return new ArrayList<>(data);
    }

    public static void test(Configuration configuration, Path logPath, List jobManagerList, List taskmanagerList, String jobId,List<String> containerId) throws Exception {
        AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(configuration, logPath);


//        AggregatedLogFormat.ContainerLogsReader logReader = reader
//                .getContainerLogsReader(ContainerId.fromString("container_e08_1678452962879_122669_01_000007"));

        AggregatedLogFormat.ContainerLogsReader logReader = null;
        AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
        DataInputStream valueStream;
        for (valueStream = reader.next(key); valueStream != null; valueStream = reader.next(key)) {
            //读取container下的日志
            String contaierId = key.toString();
            logReader = new AggregatedLogFormat.ContainerLogsReader(valueStream);

            String logType = logReader.nextLog();
            int bufferSize = 65536;
            char[] cbuf = new char[bufferSize];

            ArrayList<String> strings = new ArrayList<>();
            //读取这个container下的所有日志文件
            while (logType != null) {
                String logName = logReader.getCurrentLogType();
                //JobManager的日志需要
                if (logName.contains("jobmanager")) {
                    jobManagerList.add(logName);
                } else if(containerId.contains(contaierId)){
                    //如果containerid是这个jobid的 就只能添加这个jobID开头的日志 或者 是TaskManager开头的
                     if (logName.startsWith(jobId) || logName.startsWith("taskmanager")) {
                        taskmanagerList.add(logName);
                    }else{
                         logType = logReader.nextLog();
                         continue;
                     }
                } else {
                    break;
                }
                StringBuilder stringBuilder = new StringBuilder();
                long logLength = logReader.getCurrentLogLength();

                long start = 0;
                long end = logLength;

                long toRead = end - start;

                long totalSkipped = 0;
                while (totalSkipped < start) {
                    long ret = logReader.skip(start - totalSkipped);
                    if (ret == 0) {
                        //Read one byte
                        int nextByte = logReader.read();
                        // Check if we have reached EOF
                        if (nextByte == -1) {
                            throw new IOException("Premature EOF from container log");
                        }
                        ret = 1;
                    }
                    totalSkipped += ret;
                }

                int len = 0;
                int currentToRead = toRead > bufferSize ? bufferSize : (int) toRead;

                while (toRead > 0
                        && (len = logReader.read(cbuf, 0, currentToRead)) > 0) {
                    stringBuilder.append(new String(cbuf, 0, len));
                    toRead = toRead - len;
                    currentToRead = toRead > bufferSize ? bufferSize : (int) toRead;
                }
                strings.add(stringBuilder.toString());

                logType = logReader.nextLog();
            }
//        String s2 = HttpUtil.get("http://hadoop-node2:18042/node/containerlogs/container_e08_1678452962879_121990_01_000002/admin/0d74290ba1fbf47abe44e66ae2a6fd8d_taskmanager.log?start=0");

            System.out.println(strings);
        }
    }
}
