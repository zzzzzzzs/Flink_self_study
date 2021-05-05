package com.me.transform;


import com.me.bean.SensorReading;
import com.me.source.SensorSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// TODO map语义 针对流中的每一个元素进行一对一的转换操作
public class transform_02_map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        // 匿名函数
        stream.map(r -> r.id).print("lambda map");

        // 推荐使用Java的写法
        stream
                .map(new MapFunction<SensorReading, String>() {
                    @Override
                    public String map(SensorReading value) throws Exception {
                        return value.id;
                    }
                })
                .print("map");

        stream.map(new MyMap()).print();

        // 使用flatMap来实现map的功能
        // flatMap是map和filter的泛化
        // flatMap是比map和filter更加底层的算子，可以用flatMap来实现map和filter的功能
        // TODO flatMap的语义：将列表中的每一个元素转化成0个，1个或者多个元素
        stream
                .flatMap(new FlatMapFunction<SensorReading, String>() {
                    @Override
                    public void flatMap(SensorReading value, Collector<String> out) throws Exception {
                        out.collect(value.id);
                    }
                })
                .print("flatmap");

        env.execute();
    }

    public static class MyMap implements MapFunction<SensorReading, String> {
        @Override
        public String map(SensorReading value) throws Exception {
            return value.id;
        }
    }
}
