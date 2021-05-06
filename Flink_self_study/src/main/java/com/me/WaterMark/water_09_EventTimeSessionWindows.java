package com.me.WaterMark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class water_09_EventTimeSessionWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        /* TODO EventTimeSessionWindows 事件会话窗口
            相邻两次数据的 EventTime 的时间差超过指定的时间间隔就会触发执行。
            如果加入 Watermark， 会在符合窗口触发的情况下进行延迟。到达延迟水位再进行窗口触发。
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
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(500)))
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
