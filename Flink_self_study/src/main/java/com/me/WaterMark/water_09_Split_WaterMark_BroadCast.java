package com.me.WaterMark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;



/*
    TODO 分流水位线传播
     先设置水位线 -> 分组 -> 开事件窗口（5s）-> 处理
     数据源格式：a 1  【word 事件时间】 ->  map：(word 事件时间)
     当word不一样经过keyBY就会将分区（逻辑分区），然后最大水位线就会复制广播到后面的所有的逻辑分区（有可能不在同一个槽中，但是也会接收到同样的水位线）
* */

public class water_09_Split_WaterMark_BroadCast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);


        env.setParallelism(1);

        /*
        TODO 为什么不设置分区或者并行度设置成3（就是用默认的核数）会有一个延迟时间呢？???????
            并行度1--->  输入 a 5 输出
                 2           a 6 输出
                 3           a 7 输出
                 why ????
                 水位线 = 观察到的元素中的最大时间戳 - 最大延迟时间 - 1ms
        */

        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        System.out.println(Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L).toString());
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
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                        collector.collect(s + "窗口共有" + iterable.spliterator().getExactSizeIfKnown() + "条数据");
                    }
                })
                .print();

        env.execute();
    }
}
