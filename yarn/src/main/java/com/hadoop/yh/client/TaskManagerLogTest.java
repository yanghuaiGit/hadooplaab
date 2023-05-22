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
import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.example.util.HttpUtil;
import org.example.util.UrlUtil;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TaskManagerLogTest {
    public static final String YARN_APPLICATION_URL_FORMAT = "%s/ws/v1/cluster/apps/%s";

    public static final String YARN_CONTAINER_LOG_URL_FORMAT = "%s/node/containerlogs/%s/%s";

    private static final Pattern ERR_INFO_BYTE_PATTERN =
            Pattern.compile("(?<name>[^:]+):+\\s+[a-zA-Z\\s]+(\\d+)\\s*bytes");

    private static final String LOG_FILE_REGEX = "[_a-zA-Z0-9]+\\.(log|err|out)(\\.\\d+)?";
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String applicationId = "application_1681649509593_0149";
        String jobId = "c46e903dd605b9a5d9d96853f15c8f33";
        String archiveDir = "hdfs://ns1/dtInsight/flink110/completed-jobs";
        String hdfsFs = "hdfs://ns1";
        String yarnUrl = "http://devops-hadoop2-node3:18088/";
        String s = HttpUtil.get(String.format(YARN_APPLICATION_URL_FORMAT, yarnUrl, applicationId));
        Map<String, Object> applicationWsParser = OBJ_MAPPER.readValue(s, Map.class);
        applicationWsParser = (Map<String, Object>) applicationWsParser.get("app");

        Object state = applicationWsParser.get("state");

        ArrayList<RollingBaseInfo> rollingBaseInfos = new ArrayList<>();

        String amContainerLogsURL = applicationWsParser.get("amContainerLogs").toString();
        String logPreURL = UrlUtil.getHttpRootUrl(amContainerLogsURL);
        RollingBaseInfo rollingBaseInfo = test(amContainerLogsURL, logPreURL, "jobmanager");


        rollingBaseInfos.add(rollingBaseInfo);

        if (state.toString().equals("RUNNING")) {

            String user = applicationWsParser.get("user").toString();
            String trackingUrl = applicationWsParser.get("trackingUrl").toString();

            String containerLogUrlFormat =
                    UrlUtil.formatUrlHost(amContainerLogsURL);


            //找到所有的任务的id session任务这么获取
            List<String[]>containers = new ArrayList<>();
            /**
             * session任务尝试从jobArchive里获取taskManagerContainer信息来获取日志信息
             * 示例:http://xxxx:18088/proxy/application_1678452962879_122714/jobs/20a88025ef323fcbd4e64aea63b82073
             * {"jid":"20a88025ef323fcbd4e64aea63b82073","name":"runJob_run_sync_task_stream01_1681455593279_20230414145953","isStoppable":false,"state":"FINISHED","start-time":1681455595522,"end-time":1681455680800,"duration":85278,"now":1681455890762,"timestamps":{"RUNNING":1681455596047,"CREATED":1681455595559,"FAILING":0,"FINISHED":1681455680800,"CANCELED":0,"RECONCILING":0,"CANCELLING":0,"FAILED":0,"INITIALIZING":1681455595522,"RESTARTING":0,"SUSPENDED":0},"vertices":[{"id":"bc764cd8ddf7a0cff126f51c16239658","name":"Source: streamsourcefactory","parallelism":1,"status":"FINISHED","start-time":1681455600199,"end-time":1681455660797,"duration":60598,"tasks":{"FAILED":0,"DEPLOYING":0,"CANCELED":0,"RUNNING":0,"CANCELING":0,"RECONCILING":0,"SCHEDULED":0,"FINISHED":1,"CREATED":0},"metrics":{"read-bytes":0,"read-bytes-complete":true,"write-bytes":1569,"write-bytes-complete":true,"read-records":0,"read-records-complete":true,"write-records":40,"write-records-complete":true}},{"id":"0a448493b4782967b150582570326227","name":"Sink: streamsinkfactory","parallelism":1,"status":"FINISHED","start-time":1681455600200,"end-time":1681455680798,"duration":80598,"tasks":{"FAILED":0,"DEPLOYING":0,"CANCELED":0,"RUNNING":0,"CANCELING":0,"RECONCILING":0,"SCHEDULED":0,"FINISHED":1,"CREATED":0},"metrics":{"read-bytes":1573,"read-bytes-complete":true,"write-bytes":0,"write-bytes-complete":true,"read-records":40,"read-records-complete":true,"write-records":0,"write-records-complete":true}}],"status-counts":{"FAILED":0,"DEPLOYING":0,"CANCELED":0,"RUNNING":0,"CANCELING":0,"RECONCILING":0,"SCHEDULED":0,"FINISHED":2,"CREATED":0},"plan":{"jid":"20a88025ef323fcbd4e64aea63b82073","name":"runJob_run_sync_task_stream01_1681455593279_20230414145953","nodes":[{"id":"0a448493b4782967b150582570326227","parallelism":1,"operator":"","operator_strategy":"","description":"Sink: streamsinkfactory","inputs":[{"num":0,"id":"bc764cd8ddf7a0cff126f51c16239658","ship_strategy":"FORWARD","exchange":"pipelined_bounded"}],"optimizer_properties":{}},{"id":"bc764cd8ddf7a0cff126f51c16239658","parallelism":1,"operator":"","operator_strategy":"","description":"Source: streamsourcefactory","optimizer_properties":{}}]}}
             */
            String jobInfo = HttpUtil.get(String.format("%s/jobs/%s", trackingUrl, jobId));
            if (StringUtils.isNotBlank("jobInfo")) {
                try {
                    Map<String, Object> jobMap = OBJ_MAPPER.readValue(jobInfo,Map.class);
                    Object vertices = jobMap.get("vertices");
                    if (vertices instanceof List) {
                        List<Map<String, Object>> verticeList = (List<Map<String, Object>>) vertices;
                        if (CollectionUtils.isNotEmpty(verticeList)) {
                            for (Object verticeId : verticeList.stream().map(i -> i.get("id")).filter(Objects::nonNull).collect(Collectors.toList())) {
                                String taskmanagerinfo =  HttpUtil.get(String.format("%s/jobs/%s/vertices/%s/taskmanagers", trackingUrl, jobId,verticeId));
                                Map<String, Object> taskmanagerMap = OBJ_MAPPER.readValue(taskmanagerinfo,Map.class);
                                Object taskmanagers = taskmanagerMap.get("taskmanagers");
                                if (taskmanagers instanceof List) {
                                    List<Map<String, Object>> taskmanagerList = (List<Map<String, Object>>) taskmanagers;

                                    for (Map<String, Object> stringObjectMap : taskmanagerList) {
                                        Object containerId = stringObjectMap.get("taskmanager-id");
                                        Object host = stringObjectMap.get("host");
                                        if (containerId != null && host != null) {
                                            String[] nameAndHost = new String[2];
                                            nameAndHost[0] = containerId.toString();
                                            if (host.toString().contains(":")) {
                                                nameAndHost[1] = host.toString().split(":")[0];
                                            } else {
                                                nameAndHost[1] = host.toString();
                                            }
                                            containers.add(nameAndHost);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    //containers过滤
                    HashSet<Object> objects = new HashSet<>();
                    containers = containers.stream().map(i->String.join(":",i)).distinct().map(i->i.split(":")).collect(Collectors.toList());
                    for (String[] nameAndHost1 : containers) {
                        String containerUlrPre1 = String.format(containerLogUrlFormat, nameAndHost1[1]);
                        String containerLogUrl1 = String.format(
                                YARN_CONTAINER_LOG_URL_FORMAT,
                                containerUlrPre1,
                                nameAndHost1[0],
                                user);

                        String preUrl1 = UrlUtil.getHttpRootUrl(containerUlrPre1);

                        RollingBaseInfo rollingBaseInfo12 = test(containerLogUrl1, preUrl1, "taskmanager");
                        rollingBaseInfos.add(rollingBaseInfo12);
                    }
                } catch (Exception e) {
//                    LOG.info("parse data error");
                }
            }
            //获取当前运行的taskmanager的日志信息
//            String format = String.format("%s/taskmanagers", trackingUrl);
//            String s1 = HttpUtil.get(format);
//            Map<String, Object> response = OBJ_MAPPER.readValue(s1, Map.class);
//            List<Map<String, Object>> taskmanagers = (List) response.get("taskmanagers");
//            for (Map<String, Object> taskmanager : taskmanagers) {
//                String[] nameAndHost = parseContainerNameAndHost(taskmanager);
//                String containerUlrPre = String.format(containerLogUrlFormat, nameAndHost[1]);
//                String containerLogUrl = String.format(
//                        YARN_CONTAINER_LOG_URL_FORMAT,
//                        containerUlrPre,
//                        nameAndHost[0],
//                        user);
//
//                String preUrl = UrlUtil.getHttpRootUrl(containerLogUrl);
//
//                RollingBaseInfo rollingBaseInfo1 = test(containerLogUrl, preUrl, "taskmanager");
//                rollingBaseInfos.add(rollingBaseInfo1);
//            }

        } else {
            //任务结束 只能拿到taskmanager的日志信息 jobManager的信息是拿不到的？ 是session集群关闭吗 还是其余的呢
            String user = applicationWsParser.get("user").toString();
            String containerLogUrlFormat =
                    UrlUtil.formatUrlHost(amContainerLogsURL);


            //开始解析归档日志
            List<String[]> taskManagerLogInfoFromJobArchive =
                    getTaskManagerLogInfoFromJobArchive(jobId, archiveDir, hdfsFs);

            for (String[] nameAndHost1 : taskManagerLogInfoFromJobArchive) {
                String containerUlrPre1 = String.format(containerLogUrlFormat, nameAndHost1[1]);
                String containerLogUrl1 = String.format(
                        YARN_CONTAINER_LOG_URL_FORMAT,
                        containerUlrPre1,
                        nameAndHost1[0],
                        user);

                String preUrl1 = UrlUtil.getHttpRootUrl(containerUlrPre1);

                RollingBaseInfo rollingBaseInfo12 = test(containerLogUrl1, preUrl1, "taskmanager");
                rollingBaseInfos.add(rollingBaseInfo12);
            }

        }
        //如果开启了日志隔离 需要过滤掉其余的任务的日志
        for (RollingBaseInfo rollingBaseInfo1 : rollingBaseInfos) {
            boolean hasSessionLogSplit = false;
            ArrayList<LogBaseInfo> tempLogBaseInfos = new ArrayList<>();
            for (LogBaseInfo log : rollingBaseInfo1.getLogs()) {
                if (log.getName().contains(jobId)) {
                    hasSessionLogSplit = true;
                    tempLogBaseInfos.add(log);
                } else if (log.getName().equals("taskmanager.log") || log.getName().equals("taskmanager.out") || log.getName().equals("taskmanager.err")) {
                    tempLogBaseInfos.add(log);
                }
            }
            if (hasSessionLogSplit) {
                rollingBaseInfo1.setLogs(tempLogBaseInfos);
            }
        }
        System.out.println(OBJ_MAPPER.writeValueAsString(rollingBaseInfos));
    }


    public static List<String[]> getTaskManagerLogInfoFromJobArchive(String jobId, String archiveDir, String hdfsFs) {

        try {
            String path = archiveDir + "/" + jobId;
            URI uri = new URI(hdfsFs);
            Configuration configuration = new Configuration();
            configuration.addResource("devops/core-site.xml");
            configuration.addResource("devops/hdfs-site.xml");
            configuration.addResource("devops/yarn-site.xml");
            FileSystem fs = FileSystem.get(uri, configuration);
            Path hdfsFilePath = new Path(path);
            if (!fs.exists(hdfsFilePath)) {
                return Collections.EMPTY_LIST;
            }
            FSDataInputStream open = fs.open(hdfsFilePath);
            InputStreamReader reader = new InputStreamReader(open, StandardCharsets.UTF_8);
            Map<String, Object> jobArchiveAll = OBJ_MAPPER.readValue(reader, Map.class);

            // todo: json解析映射应该更规范化
            List<Map<String, Object>> jsonArray = (List<Map<String, Object>>) jobArchiveAll.get("archive");
            List<String> verticesUrls = new ArrayList<>();
            for (Map<String, Object> ele : jsonArray) {
                /// get job inforMation from jobs/jobId
                if (StringUtils.equals(
                        ele.get("path").toString(),
                        String.format("/jobs/%s", jobId))) {
                    String exception = ele.get("json").toString();
                    if (StringUtils.isNotBlank(exception)) {
                        JsonParser jsonParser = new JsonParser();
                        JsonElement element = jsonParser.parse(exception);
                        if (!element.isJsonObject()) {
                            continue;
                        }
                        JsonObject parse = (JsonObject) element;
                    // 如果任务运行中 然后session集群被kill掉了 这个时候 归档日志里的vertices是null的
                        for (JsonElement vertices : parse.getAsJsonArray("vertices")) {
                            JsonObject asJsonObject = vertices.getAsJsonObject();
                            verticesUrls.add(
                                    String.format(
                                            "/jobs/%s/vertices/%s",
                                            jobId,
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
            //
            // "path":"/jobs/5f14320f061bf3501b647760648cb3e8/vertices/bc764cd8ddf7a0cff126f51c16239658",
            //            "json":"{\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"name\":\"Source:
            // streamsourcefactory\",\"parallelism\":1,\"now\":1679239165429,\"subtasks\":[{\"subtask\":0,\"status\":\"FINISHED\",\"attempt\":0,\"host\":\"172-16-23-232\",\"start-time\":1679239024924,\"end-time\":1679239145412,\"duration\":120488,\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":true,\"write-bytes\":3313,\"write-bytes-complete\":true,\"read-records\":0,\"read-records-complete\":true,\"write-records\":100,\"write-records-complete\":true},\"taskmanager-id\":\"container_e20_1677137002310_366101_01_000013\",\"start_time\":1679239024924}]}"
            //        }
            List<String[]> data = new ArrayList<>();
            HashSet<String> taskManagerIds = new HashSet<>();
            for (Map<String, Object> ele : jsonArray) {
                if (verticesUrls.contains(ele.get("path").toString())) {
                    String exception = ele.get("json").toString();
                    if (StringUtils.isNotBlank(exception)) {
                        JsonParser jsonParser = new JsonParser();
                        JsonElement element = jsonParser.parse(exception);
                        if (!element.isJsonObject()) {
                            continue;
                        }
                        JsonObject parse = (JsonObject) element;
                        for (JsonElement subtasks : parse.get("subtasks").getAsJsonArray()) {
                            JsonObject subTask = subtasks.getAsJsonObject();
                            String containerId = subTask.get("taskmanager-id").getAsString();
                            String host = subTask.get("host").getAsString();
                            if (!taskManagerIds.contains(containerId + host)) {
                                taskManagerIds.add(containerId + host);
                                String[] strings = new String[2];
                                strings[0] = containerId;
                                strings[1] = host;
                                data.add(strings);
                            }
                        }
                    }
                }
            }
            return data;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static RollingBaseInfo test(String amContainerLogsURL, String logPreURL, String type) {
        String amContainerPreViewHttp = HttpUtil.get(amContainerLogsURL);

        org.jsoup.nodes.Document document = Jsoup.parse(amContainerPreViewHttp);

        Elements el = document.getElementsByClass("content");
        if (el.size() < 1) {
            throw new RuntimeException("httpText don't have any ele in http class : content");
        }

        Elements afs = el.get(0).select("a[href]");

        Elements pfs = el.get(0).select("p");

        if (afs.size() < 1) {
            throw new RuntimeException(
                    "httpText data format not correct, please check task status or url response content!!");
        }
        RollingBaseInfo rollingBaseInfo = new RollingBaseInfo();
        rollingBaseInfo.setTypeName(type);
        HashSet<String> logNames = new HashSet<>();
        for (org.jsoup.nodes.Element af : afs) {
            String logURL = af.attr("href");
            // 截取url参数部分
            logURL = logURL.substring(0, logURL.indexOf("?"));
            logURL = logPreURL + logURL;
            //如果任务已经归档了 应该是这个logURL 前面在加上yarn.log.server.url的值
            /**
             * <property>
             * <name>yarn.log.server.url</name>
             * <value>http://172.16.83.73:19888/jobhistory/logs/</value>
             * <final>false</final>
             * <source>yarn-site.xml</source>
             * </property>
             */
            String jobErrByteStr = af.text();

            String infoTotalBytes = "0";
            String logName = "";

            Matcher matcher = ERR_INFO_BYTE_PATTERN.matcher(jobErrByteStr);
            if (matcher.find()) {
                logName = StringUtils.trimToEmpty(matcher.group(1));

                infoTotalBytes = matcher.group(2);
            }

            if (logName.matches(LOG_FILE_REGEX)) {
                rollingBaseInfo.addLogBaseInfo(new LogBaseInfo(logName, logURL, infoTotalBytes));
            } else if (jobErrByteStr.equals("here") && pfs.size() > 0) {
                boolean b = false;
                boolean all = false;
                for (Element pf : pfs) {
                    String text = pf.text();
                    String substring = af.attr("href").substring(0, af.attr("href").indexOf("?"));

                    if (text.startsWith("Log Type:")) {
                        String name = text.substring(9).trim();
                        if (substring.endsWith(name + "/")) {
                            logName = name;
                            logURL = amContainerLogsURL + "/" + logName;
                            b = true;
                        }
                    } else if (b && text.startsWith("Log Length:")) {
                        String length = text.substring(10).trim();
                        infoTotalBytes = length;
                        all = true;
                    }

                    if (all && !logNames.contains(logName)) {
                        logNames.add(logName);
                        rollingBaseInfo.addLogBaseInfo(new LogBaseInfo(logName, logURL, infoTotalBytes));
                    }
                }
            }
        }
        return rollingBaseInfo;
    }

    private static String[] parseContainerNameAndHost(Map<String, Object> taskmanagerInfo) {
        String containerName = taskmanagerInfo.get("id").toString();
        String akkaPath = taskmanagerInfo.get("path").toString();
        String host = "";
        try {

            host = akkaPath.split("[@:]")[2];
        } catch (Exception e) {
            System.out.println(e);
        }
        return new String[]{containerName, host};
    }

    public static class LogBaseInfo {
        String name;
        String url;
        String totalBytes;

        public LogBaseInfo() {
        }

        public LogBaseInfo(String name, String url, String totalBytes) {
            this.name = name;
            this.url = url;
            this.totalBytes = totalBytes;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getTotalBytes() {
            return totalBytes;
        }

        public void setTotalBytes(String totalBytes) {
            this.totalBytes = totalBytes;
        }

        @Override
        public String toString() {
            return "LogBaseInfo{"
                    + "name='"
                    + name
                    + '\''
                    + ", url='"
                    + url
                    + '\''
                    + ", totalBytes="
                    + totalBytes
                    + '}';
        }
    }

    public static class RollingBaseInfo {
        String typeName;
        List<LogBaseInfo> logs = Lists.newArrayList();
        String otherInfo;

        public RollingBaseInfo() {
        }

        public RollingBaseInfo(String typeName, List<LogBaseInfo> logs) {
            this.typeName = typeName;
            this.logs = logs;
        }

        public void addLogBaseInfo(LogBaseInfo logBaseInfo) {
            this.logs.add(logBaseInfo);
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public List<LogBaseInfo> getLogs() {
            return logs;
        }

        public void setLogs(List<LogBaseInfo> logs) {
            this.logs = logs;
        }

        public String getOtherInfo() {
            return otherInfo;
        }

        public void setOtherInfo(String otherInfo) {
            this.otherInfo = otherInfo;
        }

        @Override
        public String toString() {
            return "RollingBaseInfo{" +
                    "typeName='" + typeName + '\'' +
                    ", logs=" + logs +
                    ", otherInfo='" + otherInfo + '\'' +
                    '}';
        }
    }


}
