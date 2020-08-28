package com.yh.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class SimpleMain {
    public static void main(String[] args) throws Exception {
        String input = "/Users/yanghuai/Desktop/test.txt";
        // 获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.readTextFile(input);
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split("\t");
                for (String token : tokens) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }).groupBy(0).sum(1).print();
    }
}
