package com.yh.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class SimpleMain extends BaseContext{

    @Test
    public  void main() throws Exception {
        StreamExecutionEnvironment env = getEnv();
        String path = SimpleMain.class.getClassLoader().getResource("test.txt").getPath();
        DataStreamSource<String> text = env.readTextFile(path);
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                for (String token : tokens) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }).print();
        env.execute();
    }
}
