package com.me.test;

import com.me.bean.Word;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class water_01_WatermarkStrategy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                // 输入a 1
                .socketTextStream("bigdata102", 9999)
                .map(new MapFunction<String, Word>() {
                    @Override
                    public Word map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return new Word(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Word>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Word>() {
                                    @Override
                                    public long extractTimestamp(Word element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }))
                .keyBy(r -> r.word)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Word, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Word> iterable, Collector<String> collector) throws Exception {
                        collector.collect(context.window().getStart() + "~" + context.window().getEnd() + "窗口中有 " + iterable.spliterator().getExactSizeIfKnown() + " 条数据");
                    }
                })
                .print();

        env.execute();
    }


}
