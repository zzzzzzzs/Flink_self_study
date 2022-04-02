package com.me.study.WaterMark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;



/*
    TODO 合流水位线传播
* */

public class water_06_Merge_WaterMark_BroadCast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env
                .socketTextStream("bigdata102", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                );

        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env
                .socketTextStream("bigdata102", 9998)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                );

        stream1
                .union(stream2)
                /*
                    TODO 一条流用ProcessFunction
                         keyBy 用  KeyedProcessFunction
                         keyBy.window 用 ProcessWindowFunction or aggregate

                    TODO 现在有2个条数据源，此刻union中就有2个水位线。一开始在union中的2个水位线是负无穷大。选择一个2者中较小的一个水位线输出，union中的水位线会一直更新的。
                           wm: watermark ;  x:表示水平线已经被替代了
                        现在输入                                     union                                              union                                             union
                            port 9999: [a 1] (wm:999ms) \        wm:-max (x, 999ms)\      print    none           \   wm:999ms          \    print    [a 3] wm:2999ms\   wm:999ms(x,2999ms)\     print   一次类推
                                                         ------>                    ----> -max ---                -->                   ---> -max ---                -->                    ---> 1999ms 。。。。。。
                            port 9998: none             /        wm:-max           /               [a 2] wm:1999ms/   wm:-max(x,1999ms) /             none           /   wm:1999ms         /
                        解释：一开始在union中的2个水位线是负无穷大。现在往9999端口发送[a 1] 此时水位线是999ms，9998端口没有数据。然后进入到union中，
                            此时union中的水位线分别是-max和-max，然后处理process里面的具体操作打印一个union中最小的水位线输出：-max。
                            当process函数处理完成以后更新union里面的水位线为999ms和-max。
                            现在往9998端口发送[a 2] 此时水位线是1999ms，9999端口没有数据。然后进入到union中，此时union中的水位线分别是999ms和-max。
                            然后处理process里面的具体操作打印一个union中最小的水位线输出：-max。当process函数处理完成以后更新union里面的水位线为999ms和1999ms。
                            现在往9999端口发送[a 3] 此时水位线是2999ms，9998端口没有数据。然后进入到union中，此时union中的水位线分别是999ms和1999ms。
                            然后处理process里面的具体操作打印一个union中最小的水位线输出：1999ms。然后进入到union中，此时union中的水位线分别是1999ms和2999ms。


                        但是union会将2个数据源合并成一个，
                            因此只会向下游发送1个水位线。
                * */
                .process(new ProcessFunction<Tuple2<String, Long>, String>() {
                    // 数据一来就立即触发
                    @Override
                    public void processElement(Tuple2<String, Long> stringLongTuple2, Context context, Collector<String> collector) throws Exception {
                        String s = stringLongTuple2.toString() + " 的当前水位线是：" + context.timerService().currentWatermark();
                        collector.collect(s);
                    }
                })
                .print();

        env.execute();
    }
}
