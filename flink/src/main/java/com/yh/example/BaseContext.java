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

package com.yh.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.HashMap;

public class BaseContext implements Serializable {
    protected Configuration flinkConf = new Configuration();
    // Configuration configuration = GlobalConfiguration.loadConfiguration("");

    public StreamExecutionEnvironment getEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.getConfig().setGlobalJobParameters(flinkConf);

        return env;
    }

    protected void checkPointSetting(long interval, CheckpointingMode mode, Boolean unalignedCheckpoint) {
        StreamExecutionEnvironment env = getEnv();
        env.enableCheckpointing(interval, mode);
        //execution.checkpointing.interval
        env.getCheckpointConfig().enableUnalignedCheckpoints(unalignedCheckpoint);
    }

    protected void stateBackend(String path) {
        StreamExecutionEnvironment env = getEnv();
        HashMap<String, String> conf = new HashMap<>();
        conf.put("state.backend", "ROCKSDB");
        conf.put("state.checkpoints.num-retained", "10");
        conf.put("state.checkpoints.dir", StringUtils.isBlank(path) ? "file:///tmp/ck" : path);
        env.getConfig().setGlobalJobParameters(Configuration.fromMap(conf));
    }
}
