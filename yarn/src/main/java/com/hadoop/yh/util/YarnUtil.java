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

package com.hadoop.yh.util;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.HashMap;

public class YarnUtil {
    public static Gson gson = new Gson();

    public static void main(String[] args) throws IOException {

        final CloseableHttpClient httpclient = HttpClients.createDefault();

        CloseableHttpResponse execute = httpclient.execute(new HttpGet("http://devops-hadoop2-node3:18088/ws/v1/cluster/apps/"));

        final HttpPut httpput = new HttpPut("http://hadoop-node2:18088/ws/v1/cluster/apps/%s/state");

        HashMap params = new HashMap<>();

        params.put("state", "KILLED");
        StringEntity stringEntity = new StringEntity(gson.toJson(params), "UTF-8");
        stringEntity.setContentType("application/json");
        httpput.setEntity(stringEntity);

        System.out.println("executing request " + httpput.getMethod() + " " + httpput.getURI());
        try (final CloseableHttpResponse response = httpclient.execute(httpput)) {
            System.out.println("----------------------------------------");
            System.out.println(response);

            final HttpEntity resEntity = response.getEntity();
            if (resEntity != null) {
                System.out.println("Response content length: " + resEntity.getContentLength());
            }
            try {
                EntityUtils.consume(resEntity);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

}
