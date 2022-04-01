package com.me.window;

import com.me.bean.SensorReading;
import com.me.source.SensorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;


/*
TODO    ProcessWindowFunction(全窗口函数)
    ProcessWindowFunction 可以得到一个包含这个窗口中所有元素的迭代器, 以及这些元素所属窗口的一些元数据信息，例如Flink的上下文Context
    ProcessWindowFunction不能被高效执行的原因是Flink在执行这个函数之前, 需要在内部缓存这个窗口上所有的元素.
        ReduceFunction,AggregateFunction更加高效, 原因就是Flink可以对到来的元素进行增量聚合 .
            会在窗口结束的时候调用
* */
public class window7_function_ProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*
        TODO 一般操作都是先过滤，分组，开窗，聚合
            此需求是使用AggregateFunction方法求5秒内窗口的平均温度
        * */

        env.setParallelism(1);
        env
                .addSource(new SensorSource())
                .filter(r -> r.id.equals("sensor_1"))
                .keyBy(r -> r.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<SensorReading, Avg, String, TimeWindow> {
        // 参数1: key 参数2: 上下文对象 参数3: 迭代器 这个窗口内所有的元素 参数4: 收集器, 用于向下游传递数据

        @Override
        public void process(String key, Context context, Iterable<SensorReading> iterable, Collector<Avg> collector) throws Exception {
            double sum = 0.0;
            long count = 0L;
            for (SensorReading r : iterable) {
                sum += r.temperature;
                count += 1L;
            }
            collector.collect(new Avg(key, sum / count, context.window().getStart(), context.window().getEnd()));
        }
    }

    public static class Avg {
        public String id;
        public Double avg;
        public Long windowStart;
        public Long windowEnd;

        public Avg() {
        }

        public Avg(String id, Double avg, Long windowStart, Long windowEnd) {
            this.id = id;
            this.avg = avg;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "Avg{" +
                    "id='" + id + '\'' +
                    ", avg=" + avg +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }
}
