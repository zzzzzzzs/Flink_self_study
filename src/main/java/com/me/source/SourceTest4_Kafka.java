package com.me.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SourceTest4_Kafka {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);

    /*
    TODO 先启动本程序，然后启动一个kafka的生产者，观察窗口数据
    * */
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "consumer-group");
    properties.setProperty(
        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty(
        "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty("auto.offset.reset", "latest");

    // TODO 从kafka中读取数据 一般是流处理
    DataStream<String> dataStream =
        env.addSource(
            (new FlinkKafkaConsumer<>("ods_base_log", new SimpleStringSchema(), properties)));

    // 打印输出
    dataStream.print();

    env.execute();
  }
}
