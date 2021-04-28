package com.me.WaterMark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class bbb {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        streamSource
                .map(n -> {
                    String[] s = n.split(" ");
                    return new Word(s[0], Long.valueOf(s[1]) * 1000);
                })
                //为何不需要返回参数，因为没有泛型
                .assignTimestampsAndWatermarks(     //该函数只是起到更新当前系统的水位线的功能，不拦截数据
                        //乱序数据插入水位线
                        //最大延迟时间设置为5s
                        //默认200ms的机器时间插入一个水位线
                        WatermarkStrategy.<Word>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Word>() {
                                    //抽取时间戳逻辑
                                    //为什么抽取时间戳逻辑
                                    //如果不指明元素中的时间戳字段，程序就不知道时间戳是哪个字段
                                    //时间戳的单位是必须是毫秒
                                    @Override
                                    public long extractTimestamp(Word element, long recordTimestamp) {
                                        return element.timeStamp;
                                    }
                                })
                )
                .keyBy(w -> {
                    System.out.println("**********" + w.name + "*********");
                    return w.name;
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))//设置事件时间的宽度
                .process(new ProcessWindowFunction<Word, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Word> elements, Collector<String> out) throws Exception {
                        String str = "该窗口的名称：" + s;
                        str += "该窗口元素的个数是：" + elements.spliterator().getExactSizeIfKnown() + "\t";

                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-HH-dd hh:mm:ss");
                        String startTime = simpleDateFormat.format(new Date(context.window().getStart()));
                        String endTime = simpleDateFormat.format(new Date(context.window().getEnd()));

                        str += "\t 开始时间：" + startTime + ",\t 结束时间" + endTime;
                        out.collect(str);
                    }
                }).print();
        env.execute();
    }

    private static class Word {
        private String name;
        private Long timeStamp;

        public Word() {
        }

        public Word(String name, Long timeStamp) {
            this.name = name;
            this.timeStamp = timeStamp;
        }

        @Override
        public String toString() {
            return "Word{" +
                    "name='" + name + '\'' +
                    ", timeStamp=" + timeStamp +
                    '}';
        }
    }
}
