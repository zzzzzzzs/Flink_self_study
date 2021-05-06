package com.me.ProcessFunctionAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessFunctionAPI_01_ProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        /*
        TODO ProcessFunction 来一条数据就处理一条
             比KeyedProcessFunction少一个key的信息
        * */
        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] ss = value.split(" ");
                        return Tuple2.of(ss[0], Integer.valueOf(ss[1]));
                    }
                })
                .process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(value);
                    }
                })
                .print();
        env.execute();
    }
}
