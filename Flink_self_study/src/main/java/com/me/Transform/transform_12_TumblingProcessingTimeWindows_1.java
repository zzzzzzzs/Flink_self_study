package com.me.Transform;



import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/*
TODO 窗口函数 Tumbling Windows 滚动窗口
	滚动窗口有固定的大小, 窗口与窗口之间不会重叠也没有缝隙.比如,如果指定一个长度为5分钟的滚动窗口,
	当前窗口开始计算, 每5分钟启动一个新的窗口.
	滚动窗口能将数据流切分成不重叠的窗口，每一个事件只能属于一个窗口。
* */
public class transform_12_TumblingProcessingTimeWindows_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // TODO 如果只有一个并行度，那么所有数据都在同一个窗口，在一个窗口的数据才可以进行操作
        env.setParallelism(1);
        env
                .addSource(new SensorSource())
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                        return SensorReading.of("sensor", value1.temperature + value2.temperature, 0L);
                    }
                })
                .print();

        env.execute();
    }
}
