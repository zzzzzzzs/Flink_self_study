package com.me.study.window;



import com.me.study.bean.SensorReading;
import com.me.study.source.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/*
TODO 窗口函数 Sliding Windows 滑动窗口
    与滚动窗口一样, 滑动窗口也是有固定的长度. 另外一个参数我们叫滑动步长, 用来控制滑动窗口启动的频率.
    所以, 如果滑动步长小于窗口长度, 滑动窗口会重叠. 这种情况下, 一个元素可能会被分配到多个窗口中
    例如, 滑动窗口长度10分钟, 滑动步长5分钟, 则, 每5分钟会得到一个包含最近10分钟的数据.
* */
public class window2_ProcessTime_SlidingWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, Double, Long>> stream = env
                .addSource(new SensorSource())
                .filter(r -> r.id.equals("sensor_1"))
                .map(new MapFunction<SensorReading, Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> map(SensorReading value) throws Exception {
                        return Tuple3.of(value.id, value.temperature, 1L);
                    }
                });

        KeyedStream<Tuple3<String, Double, Long>, String> keyedStream = stream.keyBy(r -> r.f0);

        // 每一个窗口都有一个累加器
        // 当窗口闭合时，向下游发送累加器的值
        keyedStream
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> value1, Tuple3<String, Double, Long> value2) throws Exception {
                        return Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
                    }
                })
                .map(new MapFunction<Tuple3<String, Double, Long>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple3<String, Double, Long> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1 / value.f2);
                    }
                })
                .print();

        env.execute();
    }
}
