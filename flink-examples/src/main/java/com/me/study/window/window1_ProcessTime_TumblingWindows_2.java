package com.me.study.window;



import com.me.study.bean.SensorReading;
import com.me.study.source.SensorSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/*
TODO 窗口函数 Sliding Windows 滚动窗口
    与滚动窗口一样, 滑动窗口也是有固定的长度. 另外一个参数我们叫滑动步长, 用来控制滑动窗口启动的频率.
    所以, 如果滑动步长小于窗口长度, 滑动窗口会重叠. 这种情况下, 一个元素可能会被分配到多个窗口中
    例如, 滑动窗口长度10分钟, 滑动步长5分钟, 则, 每5分钟会得到一个包含最近10分钟的数据.
* */
public class window1_ProcessTime_TumblingWindows_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        // TODO 如果设置多条并行度，先分流在开窗。将所有的数据分到一个流上，然后进行开窗操作，这样下流的数据才可以进行操作。
        env.setParallelism(2);
        env
                .addSource(new SensorSource())
                .keyBy(new KeySelector<SensorReading, Boolean>() {
                    @Override
                    public Boolean getKey(SensorReading value) throws Exception {
                        return true;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
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
