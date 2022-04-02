package com.me.transform;


import com.me.bean.SensorReading;
import com.me.source.SensorSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
// spark可以使用map, flatmap, reduce, sort, groupBy可以实现其他算子所有的需求
// TODO filter语义：针对流中的每一个元素选择输出或者不输出
public class transform_03_filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        // TODO 过滤出传感器1（sensor_1）的数据


        stream.filter(r -> r.id.equals("sensor_1")).print();
        stream
                .filter(new FilterFunction<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return value.id.equals("sensor_1");
                    }
                })
                .print();
        stream.filter(new MyFilter()).print();
        // 使用flatMap来实现filter的功能
        stream
                .flatMap(new FlatMapFunction<SensorReading, SensorReading>() {
                    @Override
                    public void flatMap(SensorReading value, Collector<SensorReading> out) throws Exception {
                        if (value.id.equals("sensor_1")) {
                            out.collect(value);
                        }
                    }
                })
                .print();

        env.execute();
    }

    public static class MyFilter implements FilterFunction<SensorReading> {
        @Override
        public boolean filter(SensorReading value) throws Exception {
            return value.id.equals("sensor_1");
        }
    }
}
