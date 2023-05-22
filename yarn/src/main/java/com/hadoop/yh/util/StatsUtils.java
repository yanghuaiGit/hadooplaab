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

import java.util.Collections;
import java.util.List;
public class StatsUtils {
    public static double getMedian(List<Integer> data) {
        Collections.sort(data);
        int len = data.size();
        if (len % 2 == 0) {
            return (data.get(len / 2 - 1) + data.get(len / 2)) / 2.0;
        } else {
            return data.get(len / 2);
        }
    }
    public static double getMean(List<Integer> data) {
        double sum = 0.0;
        for (int x : data) {
            sum += x;
        }
        return sum / data.size();
    }
    public static int getMax(List<Integer> data) {
        int max = Integer.MIN_VALUE;
        for (int x : data) {
            if (x > max) {
                max = x;
            }
        }
        return max;
    }
    public static int getMin(List<Integer> data) {
        int min = Integer.MAX_VALUE;
        for (int x : data) {
            if (x < min) {
                min = x;
            }
        }
        return min;
    }
}