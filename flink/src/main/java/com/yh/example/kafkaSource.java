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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("hello", new SimpleStringSchema(), properties);

        consumer.setStartFromEarliest();
        DataStream<String> stream;
        stream = env
                .addSource(consumer);

        stream.map(i -> i + "2")
                .print("stream").setParallelism(1);
        env.execute();
    }

}
