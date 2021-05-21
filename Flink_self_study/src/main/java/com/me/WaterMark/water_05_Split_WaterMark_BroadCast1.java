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

import java.time.Duration;



/*
    TODO 分流水位线传播
     先设置水位线 -> 分组 -> 开事件窗口（5s）-> 处理
     数据源格式：a 1  【word 事件时间】 ->  map：(word 事件时间)
     当word不一样时，经过keyBY就会将分区（逻辑分区），然后水位线就会复制广播到后面的所有的逻辑分区（有可能不在同一个槽中，但是也会接收到同样的水位线）

     TODO 最好设置成1个并行度。下面按照并行度为1的情况分析：
            5s的滚动窗口，一个数据源
            [a 0] keyBy [a]          keyBy
                  ------>   --->[a 5]-----> 触发窗口，输出：a窗口共有1条数据，b窗口共有1条数据 【[a 5]这条数据会到下一个a的窗口】
            [b 4]       [b]                         上流数据水位线会复制广播到下游所有的逻辑分区中。（包括不同槽中的分区）。？？？：这里还有一些疑问：并行度不是1，这里好像就不一样了！！感觉是一个BUG
* */

public class water_05_Split_WaterMark_BroadCast1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);


        env.setParallelism(2);

        /*
         TODO bug:   如果并行度设置的不一样的话那么水位线也不一样
                比如，现在并行度是2，输入 a 0
                                       b 0
                                       a 5
                                       b 5
                     这个时候水位线到达窗口触发条件，就会打印窗口a 2条数据
                                                       窗口b 2条数据
                     但是程序运行的结果是当输入a 5的时候无法触发a窗口，当输入b 5的时候会触发a窗口和b窗口

                目前用的时候需要将并行度设置为 1
        */

        env
                .socketTextStream("bigdata102", 9999)
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
//                                .withIdleness(Duration.ofSeconds(2))
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
                        System.out.println(s);
                        collector.collect(s + "窗口共有" + iterable.spliterator().getExactSizeIfKnown() + "条数据");
                    }
                })
                .print();

        env.execute();
    }
}
