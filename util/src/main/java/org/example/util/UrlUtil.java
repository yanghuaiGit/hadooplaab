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

package org.example.util;


import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UrlUtil {
    public static final Pattern URLPattern = Pattern.compile("^(?:(http[s]?)://)?([^:/\\?]+)(?::(\\d+))?([^\\?]*)\\??(.*)");

    public static void main(String[] args) {

    }
    public UrlUtil() {
    }

    public static String getHttpUrl(String node, String path) {
        return String.format("http://%s/%s", node, path);
    }

    public static String getHttpRootUrl(String url) {
        Matcher matcher = URLPattern.matcher(url);
        if (!matcher.find()) {
            throw new RuntimeException(String.format("url:%s is not regular HTTP_URL", url));
        } else {
            String protocol = matcher.group(1) == null ? "http" : matcher.group(1);
            String hostName = matcher.group(2);
            String port = matcher.group(3);
            return port == null ? protocol + "://" + hostName : protocol + "://" + hostName + ":" + port;
        }
    }

    public static String formatUrlHost(String url) {
        Matcher matcher = URLPattern.matcher(url);
        if (!matcher.find()) {
            throw new RuntimeException(String.format("url:%s is not regular HTTP_URL", url));
        } else {
            String protocol = matcher.group(1) == null ? "http" : matcher.group(1);
            String hostNamePlaceholder = "%s";
            String port = matcher.group(3);
            return port == null ? protocol + "://" + hostNamePlaceholder : protocol + "://" + hostNamePlaceholder + ":" + port;
        }
    }
}
