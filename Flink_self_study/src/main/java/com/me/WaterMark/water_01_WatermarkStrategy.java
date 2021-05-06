package com.me.WaterMark;

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
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Word>() {
                    @Override
                    public Word map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return new Word(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                // 插入水位线的逻辑
                // TODO 必须在keyBY之前生成水平线
                .assignTimestampsAndWatermarks(
                        // 针对乱序流插入水位线
                        // 最大延迟时间设置为5s
                        // 默认200ms的机器时间插入一个水位线
                        // 水位线 = 观察到的元素中的最大时间戳 - 最大延迟时间 - 1ms
                        // TODO　水位线是一直在变化的，直到水位线到达滚动窗口的时间。并且水位线是一个单调不减的函数。可以看Watermark类
                        WatermarkStrategy.<Word>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Word>() {
                                    // 抽取时间戳的逻辑
                                    // 为什么要抽取时间戳？
                                    // 如果不指定元素中时间戳的字段，程序就不知道事件时间是哪个字段
                                    // 时间戳的单位必须是毫秒
                                    @Override
                                    public long extractTimestamp(Word element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .keyBy(r -> r.word)
                // TODO 当水位线超过设定的滚动事件窗口时间就触发执行
                //  左闭右开区间 [0, 5s) 其实就是[0, 4999ms]
                /*
                   TODO 如果水位线到达窗口的时间，那么就认为数据全部到达了
                * */
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
