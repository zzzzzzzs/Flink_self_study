package com.me.study.window;



import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.MessageFormat;


/*
TODO 窗口函数 Sliding Windows 滑动窗口
    与滚动窗口一样, 滑动窗口也是有固定的长度. 另外一个参数我们叫滑动步长, 用来控制滑动窗口启动的频率.
    所以, 如果滑动步长小于窗口长度, 滑动窗口会重叠. 这种情况下, 一个元素可能会被分配到多个窗口中
    例如, 滑动窗口长度10分钟, 滑动步长5分钟, 则, 每5分钟会得到一个包含最近10分钟的数据.
* */
public class window2_ProcessTime_SlidingWindows1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .keyBy(r->r)
                // TODO SlidingProcessingTimeWindows
                //  滑动窗口长度10s, 滑动步长5s, 则每5s会得到一个包含最近10s的数据，然后打印输出，
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String value, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        String ss = MessageFormat.format("内容：{0}，当前时间戳{1}", value, context.currentProcessingTime());
                        out.collect(ss);
                    }
                })
                .print();

        env.execute();
    }
}
