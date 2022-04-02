package com.me.study.window;



import com.me.study.bean.SensorReading;
import com.me.study.source.SensorSource;
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
public class window1_ProcessTime_TumblingWindows_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // TODO 如果只有一个并行度，那么所有数据都在同一个窗口，在一个窗口的数据才可以进行操作
        env.setParallelism(1);
        env
                .addSource(new SensorSource())
                // TODO 在非non-keyed stream上使用窗口, 流的并行度只能是1, 所有的窗口逻辑只能在一个单独的task上执行.
                //  需要注意的是: 非key分区的流, 即使把并行度设置为大于1 的数, 窗口也只能在某个分区上使用
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                        // TODO 求一个窗口内温度的总和
                        return SensorReading.of("sensor", value1.temperature + value2.temperature, 0L);
                    }
                })
                .print();

        env.execute();
    }
}
