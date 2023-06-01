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

package com.yh.example.connector.kafka;

import com.yh.example.BaseContext;
import com.yh.example.SimpleMain;
import com.yh.example.parser.SqlParser;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.util.FileUtil;
import org.junit.Test;

import java.util.Collections;

// https://blog.csdn.net/wt334502157/article/details/130171028 sql运行
public class KafkaSql extends BaseContext {
    @Test
    public void test() {
        //ticket数据 然后进行窗口划分 波动率最大的数据，求和这段时间内的每个币种的交易量，最大值 最小值，中位数
        //使用事件时间 加上 waterMark
        //这种场景最好的办法是处理时间 准确性比较高
        String path = SimpleMain.class.getClassLoader().getResource("kafka.sql").getPath();
        String sql  = FileUtil.read(path);

        StreamExecutionEnvironment env = getEnv();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        StatementSet statementSet = SqlParser.parseSql(sql, Collections.EMPTY_LIST, tableEnv);
    }
}
