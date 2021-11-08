package com.me.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;

/**
 * @author zs
 * @date 2021/11/8
 */
public class postgresCDC {
    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
                .hostname("47.110.73.221")
                .port(5432)
                .database("datacenter") // monitor postgres database
                .schemaList("public")  // monitor inventory schema
                .tableList("aaa") // monitor products table
                .username("gpadmin")
                .password("passw0rdqwe")
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(sourceFunction)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
