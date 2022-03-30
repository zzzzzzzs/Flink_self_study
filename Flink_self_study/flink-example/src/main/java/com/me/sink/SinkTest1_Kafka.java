package com.me.sink;

import com.me.bean.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*
        TODO 先启动一个kafka的消费者，然后启动本程序，然后再启动一个kafka的生产者
         在Kafka的生产者中输入 sensor_1,1547718199,35.8，然后观察kafka的消费者
         这样就形成了一个管道。
        * */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bigdata102:9092,bigdata103:9092,bigdata104:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 从kafka获取数据
        DataStream<String> inputStream = env.addSource( (new FlinkKafkaConsumer<>("flink-sensor", new SimpleStringSchema(), properties)) );

        // 转换成SensorReading类型
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Double(fields[2]), new Long(fields[1])).toString();
        });

        // TODO 将数据放入到kafka中
        dataStream.addSink(new FlinkKafkaProducer<String>("bigdata102:9092", "flinkBase", new SimpleStringSchema()));

        env.execute();
    }
}
