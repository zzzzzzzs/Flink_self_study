package com.me.transform;


import com.me.bean.SensorReading;
import com.me.source.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/* TODO 滚动聚合 reduce  有状态的操作 会保存一个累加器 输入输出是一样的，累加器是一个状态，必须维护在算子内部
    使用reduce可以实现下面的所有功能
    sum()：在输入流上对指定的字段做滚动相加操作。
    min()：在输入流上对指定的字段求最小值。
    max()：在输入流上对指定的字段求最大值。
    minBy()：在输入流上针对指定字段求最小值，并返回包含当前观察到的最小值的事件。
    maxBy()：在输入流上针对指定字段求最大值，并返回包含当前观察到的最大值的事件。
* */
public class transform_06_reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 单流
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        // 多条流
        KeyedStream<Tuple3<String, Double, Double>, String> keyedStream = stream
                .map(new MapFunction<SensorReading, Tuple3<String, Double, Double>>() {
                    @Override
                    public Tuple3<String, Double, Double> map(SensorReading value) throws Exception {
                        return Tuple3.of(value.id, value.temperature, value.temperature);
                    }
                })
                .keyBy(r -> r.f0);

        // 单条流
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> reducedStream = keyedStream
                // 每一条元素到达以后，更新累加器的值，并将累加器的值向下游发送
                // 第一个元素到来，直接作为累加器
                .reduce(new ReduceFunction<Tuple3<String, Double, Double>>() {
                    // 获取流中的最大值和流中的最小值
                    @Override
                    public Tuple3<String, Double, Double> reduce(Tuple3<String, Double, Double> value1, Tuple3<String, Double, Double> value2) throws Exception {
                        return Tuple3.of(value1.f0, Math.max(value1.f1, value2.f1), Math.min(value1.f2, value2.f2));
                    }
                });

        reducedStream.print();

        env.execute();
    }
}
