package com.me.study.lataElement;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class RedirectLateEvent_01 {
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
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                        ValueState<Boolean> isUpdated = context.windowState().getState(new ValueStateDescriptor<Boolean>("is-update", Types.BOOLEAN));
                        if (isUpdated.value() == null) {
                            // 说明窗口第一次触发计算
                            collector.collect("窗口第一次触发计算了，共 " + iterable.spliterator().getExactSizeIfKnown() + " 条元素");
                            isUpdated.update(true);
                        } else {
                            collector.collect("迟到元素到了，这时窗口共有 " + iterable.spliterator().getExactSizeIfKnown() + " 条元素");
                        }
                    }
                })
                .print();

        env.execute();
    }
}
