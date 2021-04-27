package com.me.Transform;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


/*
TODO 窗口函数 Tumbling Windows 滚动窗口
	滚动窗口有固定的大小, 窗口与窗口之间不会重叠也没有缝隙.比如,如果指定一个长度为5分钟的滚动窗口,
	当前窗口开始计算, 每5分钟启动一个新的窗口.
	滚动窗口能将数据流切分成不重叠的窗口，每一个事件只能属于一个窗口。
* */
public class transform_12_TumblingProcessingTimeWindows {
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

        // 按照传感器id分区
        KeyedStream<Tuple3<String, Double, Long>, String> keyedStream = stream.keyBy(r -> r.f0);

        WindowedStream<Tuple3<String, Double, Long>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // 每一个窗口都有一个累加器
        // 当窗口闭合时，向下游发送累加器的值
        SingleOutputStreamOperator<Tuple3<String, Double, Long>> reducedStream = windowedStream
                .reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> value1, Tuple3<String, Double, Long> value2) throws Exception {
                        return Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
                    }
                });

        // 计算窗口内数据的平均值
        reducedStream.map(new MapFunction<Tuple3<String, Double, Long>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Tuple3<String, Double, Long> value) throws Exception {
                return Tuple2.of(value.f0, value.f1 / value.f2);
            }
        }).print();

        env.execute();
    }
}
