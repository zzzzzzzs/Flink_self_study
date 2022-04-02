package com.me.study.window;

import com.me.study.bean.SensorReading;
import com.me.study.source.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;


/*
    TODO  ReduceFunction/AggregateFunction 和 ProcessWindowFunction 结合起来使用
        ReduceFunction/AggregateFunction 做增量聚合，ProcessWindowFunction 提供更多的对数据流的访问权限。
        如果只使用 ProcessWindowFunction(底层的实现为将事件都保存在 ListState 中)，将会非常占用空间。
        结合使用的话分配到某个窗口的元素将被提前聚合，而当窗口的trigger 触发时，也就是窗口收集完数据关闭时，
        将会把聚合结果发送到 ProcessWindow-Function 中，这时 Iterable 参数将会只有一个值，就是前面聚合的值。

        TODO 尽量使用增量聚合&全窗口聚合一起使用，先做预聚合
* */
public class window8_function_AggregateFunction_ProcessWindowFunction1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        /* TODO 求每个窗口的最大温度和最小温度
            使用增量聚合和全窗口聚合
        */

        env
                .addSource(new SensorSource())
                .filter(r -> r.id.equals("sensor_1"))
                .keyBy(r -> r.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 将增量聚合函数最终结果返回给ProcessWindowFunction，这样就会有更多的访问权限
                .aggregate(new Agg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class Agg implements AggregateFunction<SensorReading, Tuple2<Double, Double>, Tuple2<Double, Double>> {
        @Override
        public Tuple2<Double, Double> createAccumulator() {
            // 最大值的初始值是负无穷大
            // 最小值的初始值是无穷大
            return Tuple2.of(-Double.MAX_VALUE, Double.MAX_VALUE);
        }

        @Override
        public Tuple2<Double, Double> add(SensorReading value, Tuple2<Double, Double> accumulator) {
            return Tuple2.of(Math.max(value.temperature, accumulator.f0), Math.min(value.temperature, accumulator.f1));
        }

        // 直接输出结果
        @Override
        public Tuple2<Double, Double> getResult(Tuple2<Double, Double> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<Double, Double> merge(Tuple2<Double, Double> a, Tuple2<Double, Double> b) {
            return null;
        }
    }

    // 全窗口的输出
    public static class MaxMinTempPerWindow{
        public String id;
        public Double maxTemp;
        public Double minTemp;
        public Long windowStart;
        public Long windowEnd;

        public MaxMinTempPerWindow() {
        }

        public MaxMinTempPerWindow(String id, Double maxTemp, Double minTemp, Long windowStart, Long windowEnd) {
            this.id = id;
            this.maxTemp = maxTemp;
            this.minTemp = minTemp;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "MaxMinTempPerWindow{" +
                    "id='" + id + '\'' +
                    ", maxTemp=" + maxTemp +
                    ", minTemp=" + minTemp +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd)  +
                    '}';
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Tuple2<Double, Double>, MaxMinTempPerWindow,String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Tuple2<Double, Double>> elements, Collector<MaxMinTempPerWindow> out) throws Exception {
            Tuple2<Double, Double> tuple2 = elements.iterator().next();
            out.collect(new MaxMinTempPerWindow(s, tuple2.f0, tuple2.f1, context.window().getStart(), context.window().getEnd()));
        }
    }
}
