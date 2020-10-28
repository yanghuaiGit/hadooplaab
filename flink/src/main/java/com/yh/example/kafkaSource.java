package com.yh.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class kafkaSource {

//    static {
//        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
//        Logger root = loggerContext.getLogger("root");
//        root.setLevel(Level.INFO);
//    }

    public static void main(String[] args) throws Exception {

        //设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("hello", new SimpleStringSchema(), properties);

        consumer.setStartFromEarliest();

        //配置数据源读取数据
        DataStream<String> stream= env
                .addSource(consumer);

        stream.map(i -> i + "2")
                .print("stream").setParallelism(1);

        //提交执行
        env.execute();
    }

}
