package com.me.transform;


import com.me.bean.SensorReading;
import com.me.source.SensorSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// TODO 键控流转换算子keyBy
public class transform_05_keyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());


        /*
        TODO 基于不同的 key，流中的事件将被分配到不同的分区中去。所有具有相同 key 的事件将会在接下来的操作符的同
         一个子任务槽中进行处理。拥有不同 key 的事件可以在同一个任务中处理。但是算子只
         能访问当前事件的 key 所对应的状态。
         不能保证不同key的数据在不同的任务槽，但是相同key的事件一定在同一个任务槽，有可能导致了数据倾斜。
         Flink底层必须是分组之后才能做聚合。
         这里只有一个并行度，就一个任务槽，即使是1万个key也都在同一个任务槽。
         底层用HashMap来做逻辑分区
         */
        KeyedStream<SensorReading, String> keyedStream1 = stream.keyBy(r -> r.id);
//        KeyedStream<SensorReading, String> keyedStream1 = stream.keyBy("id");
//        KeyedStream<SensorReading, String> keyedStream1 = stream.keyBy(SensorReading::getId);


        KeyedStream<SensorReading, String> keyedStream2 = stream
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading value) throws Exception {
                        return value.id;
                    }
                });

        // 将所有数据都分到一条流上去
        KeyedStream<SensorReading, Boolean> keyedStream3 = stream
                .keyBy(new KeySelector<SensorReading, Boolean>() {
                    @Override
                    public Boolean getKey(SensorReading value) throws Exception {
                        return true;
                    }
                });

        // 将流分成奇数和偶数两条流
        env
                .fromElements(1,2,3)
                .keyBy(new KeySelector<Integer, Boolean>() {
                    @Override
                    public Boolean getKey(Integer value) throws Exception {
                        return value % 2 == 0;
                    }
                }).print();

        env.execute();
    }
}
