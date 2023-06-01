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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileUtil {

    public static String read(String path) {
        try {
            byte[] array = Files.readAllBytes(Paths.get(path));
            return new String(array, StandardCharsets.UTF_8);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /**
     * 将文本写入文件
     *
     * @param filePath 文件全路径
     * @param text 文本
     */
    public static void write(String filePath, String text) {
        // 创建文件
        File file = new File(filePath);
        delteFile(filePath);
        if (!file.exists()) {
            try {
                // 创建文件父级目录
                File parentFile = file.getParentFile();
                if (!parentFile.exists()) {
                    parentFile.mkdirs();
                }
                // 创建文件
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 将文本写入文件
        write(file, text);
    }

    public static void delteFile(String filePath) {
        new File(filePath).deleteOnExit();
    }

    /**
     * 将文本写入文件
     *
     * @param file 文件对象
     * @param text 文本
     */
    public static void write(File file, String text) {
        FileWriter writer = null;
        try {
            writer = new FileWriter(file);
            char[] chars = text.toCharArray();
            writer.write(chars);
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
