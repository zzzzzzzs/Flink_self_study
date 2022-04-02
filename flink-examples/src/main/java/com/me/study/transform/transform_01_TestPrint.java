package com.me.study.transform;

import com.me.study.bean.SensorReading;
import com.me.study.source.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 使用自定义数据源
public class transform_01_TestPrint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 测试输出
        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream.print();

        env.execute();
    }
}
