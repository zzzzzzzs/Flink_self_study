package com.me.window;

import com.me.bean.SensorReading;
import com.me.source.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/*
TODO AggregateFunction(增量聚合函数)
    此方法和强大 可以代替ReduceFunction.
    ReduceFunction & AggregateFunction更加高效, 原因就是Flink可以对到来的元素进行增量聚合 .
        reduce 输入输出类型一样
    ReduceFunction, AggregateFunction
    每条数据到来就进行计算，只保存一个简单的状态（累加器）
    当窗口闭合的时候，增量聚合完成
    处理时间：当机器时间超过窗口结束时间的时候，窗口闭合
    事件时间：当水位线超过窗口结束时间的时候，窗口闭合
    来一条数据计算一次
* */
public class window5_function_ReduceFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

            /*
             TODO 一般操作都是先过滤，分组，开窗，聚合
                此需求是使用ReduceFunction方法求5秒内窗口的平均温度
                这样做就比较麻烦，需要使用map将每条数据封装成一个元组，然后才能计算平均数
                使用AggregateFunction就会很简单
            * */
        // TODO 求sensor_1传感器 每5s的平均温度
        env
                .addSource(new SensorSource())
                .filter(r -> r.id.equals("sensor_1"))
                // 将数据封装，带有1L数量，后面reduce的时候容易计算个数
                .map(new MapFunction<SensorReading, Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> map(SensorReading value) throws Exception {
                        return Tuple3.of(value.id, value.temperature, 1L);
                    }
                })
                .keyBy(r -> r.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> value1, Tuple3<String, Double, Long> value2) throws Exception {
                        // 将温度累加，且次数累加
                        return Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
                    }
                })
                .map(new MapFunction<Tuple3<String, Double, Long>, Tuple2<String, Double>>() {
                    // 计算平均值
                    @Override
                    public Tuple2<String, Double> map(Tuple3<String, Double, Long> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1 / value.f2);
                    }
                })
                .print();

        env.execute();

    }
}
