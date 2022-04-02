package com.me.study.ProcessFunctionAPI;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;


/*
    TODO KeyedProcessFunction 和 assignTimestampsAndWatermarks 结合使用
        这样就可以使用事件时间来

    KeyedProcessFunction 要结合状态变量才可以发挥全部的威力
*
* */

public class ProcessFunctionAPI_02_KeyedProcessFunction_EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("bigdata102", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                // 设置一个水位线
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                // TODO 在 KeyedProcessFunction 中注册事件时间的定时器
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> element, Context context, Collector<String> collector) throws Exception {
                        String s = element.toString() + " 的当前水位线是：" + context.timerService().currentWatermark();
                        System.out.println(s);
//                        collector.collect(s);
                        // 每来一条数据，调用一次
                        collector.collect("数据到达，当前数据的事件时间是：" + new Timestamp(element.f1));
                        // 注册10s之后的定时器（事件时间），定时器是onTimer
                        context.timerService().registerEventTimeTimer(element.f1 + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("定时器触发了，触发时间是：" + new Timestamp(timestamp));
                    }
                })
                .print();

        env.execute();
    }
}
