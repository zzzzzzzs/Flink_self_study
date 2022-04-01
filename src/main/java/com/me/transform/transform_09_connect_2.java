package com.me.transform;


import com.me.bean.*;
import com.me.source.SensorSource;
import com.me.source.SmokeLevelSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/*
    TODO connect
    联合两条流的事件是非常常见的流处理需求。例如监控一片森林然后发出高危的火
    警警报。报警的 Application 接收两条流，一条是温度传感器传回来的数据，一条是烟雾
    传感器传回来的数据。当两条流都超过各自的阈值时，报警。
 */
public class transform_09_connect_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> sensorStream = env.addSource(new SensorSource());

        DataStreamSource<SmokeLevel> smokeSource = env.addSource(new SmokeLevelSource());

        // 每个温度传感器的数据都要和烟雾传感器联合处理
        sensorStream
                // 先进行了keyBy
                .keyBy(r -> r.id)
                // 将烟雾传感器数据广播出去
                .connect(smokeSource.broadcast())
                .flatMap(new CoFlatMapFunction<SensorReading, SmokeLevel, Alert>() {
                    // 定义变量，用来保存烟雾的水平
                    private SmokeLevel smokeLevel = SmokeLevel.LOW;

                    @Override
                    public void flatMap1(SensorReading sensorReading, Collector<Alert> collector) throws Exception {
                        // 处理来自第一条流的数据
                        if (sensorReading.temperature > 50.0 && smokeLevel == SmokeLevel.HIGH) {
                            collector.collect(new Alert(sensorReading.id + " is firing!", sensorReading.timestamp));
                        }
                    }

                    @Override
                    public void flatMap2(SmokeLevel smokeLevel, Collector<Alert> collector) throws Exception {
                        // 处理来自第二条流的数据
                        this.smokeLevel = smokeLevel;
                    }
                })
                .print();

        env.execute();
    }
}
