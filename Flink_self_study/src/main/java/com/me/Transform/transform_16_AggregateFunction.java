package com.me.Transform;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/*
TODO AggregateFunction(增量聚合函数)
    此方法和强大 可以代替ReduceFunction.
    ReduceFunction & AggregateFunction更加高效, 原因就是Flink可以对到来的元素进行增量聚合 .

    ReduceFunction, AggregateFunction
    每条数据到来就进行计算，只保存一个简单的状态（累加器）
    当窗口闭合的时候，增量聚合完成
    处理时间：当机器时间超过窗口结束时间的时候，窗口闭合
    事件时间：当水位线超过窗口结束时间的时候，窗口闭合
    来一条数据计算一次
* */
public class transform_16_AggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
         TODO 一般操作都是先过滤，分组，开窗，聚合
            此需求是使用AggregateFunction方法求5秒内窗口的平均温度
        * */

        env
                .addSource(new SensorSource())
                .filter(r -> r.id.equals("sensor_1"))
                .keyBy(r -> r.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new Agg())
                .print();

        env.execute();
    }

    public static class Agg implements AggregateFunction<SensorReading, Tuple3<String, Double, Long>, Tuple2<String, Double>> {

        // 初始化一个累加器
        @Override
        public Tuple3<String, Double, Long> createAccumulator() {
            return Tuple3.of("", 0.0, 0L);
        }

        // 将给定的输入值添加到累加器上面，返回新的累加器
        @Override
        public Tuple3<String, Double, Long> add(SensorReading value, Tuple3<String, Double, Long> accumulator) {
            return Tuple3.of(value.id, value.temperature + accumulator.f1, accumulator.f2 + 1L);
        }

        // 将结果返回，返回的是温度的平均值
        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Long> accumulator) {
            return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2);
        }

        // TODO 累加器的合并: 只有会话窗口才会调用
        @Override
        public Tuple3<String, Double, Long> merge(Tuple3<String, Double, Long> a, Tuple3<String, Double, Long> b) {
            return null;
        }
    }
}
