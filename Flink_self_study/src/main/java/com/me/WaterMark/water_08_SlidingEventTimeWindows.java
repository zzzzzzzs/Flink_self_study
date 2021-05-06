package com.me.WaterMark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class water_08_SlidingEventTimeWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        /* TODO SlidingEventTimeWindows
            这个滑动窗口主要是用来匹配不同时区的，一般时间都是以伦敦时间为主，如果想要用中国时间就可以按照注释设置。看SlidingEventTimeWindows.java的注释。
         */
        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] ss = value.split(" ");
                        return Tuple2.of(ss[0], Integer.valueOf(ss[1]) * 1000);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Integer> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }))
                .keyBy(r -> r.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(2),Time.milliseconds(500)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        out.collect(context.window().getStart() + "~" + context.window().getEnd() + "窗口中有 " + elements.spliterator().getExactSizeIfKnown() + " 条数据");
                    }
                })
                .print();


        env.execute();
    }
}
