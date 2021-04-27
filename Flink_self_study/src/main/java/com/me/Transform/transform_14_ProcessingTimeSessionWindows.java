package com.me.Transform;



import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/*
TODO 窗口函数 ProcessingTimeSessionWindows 会话窗口
    Google的框架实现了会话窗口，Flink也就实现了，开源的框架中只有Flink实现了会话窗口。
    会话窗口分配器会根据活动的元素进行分组. 会话窗口不会有重叠, 与滚动窗口和滑动窗口相比, 会话窗口也没有固定的开启和关闭时间.
	应用：捕捉会话行为，例如刷抖音，访问的时长不一样，时间不一样。早晨，晚上刷。其余时间不刷抖音。
* */
public class transform_14_ProcessingTimeSessionWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 先启动nc -lk 9999
        // windows运行：nc -lp 9999
        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Integer>() {
                    // 将获取到的字符串转成整数
                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.parseInt(value);
                    }
                })
                // 将数据汇合成一个流
                .keyBy(r -> true)
                // 开启一个静态的会话窗口，如果在规定的时间内一直有数据来，会话不会关闭，只有在规定的时间内没有任何数据进来，窗口才会关闭。
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                // 求一个会话窗口内的和
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                })
                .print();

        env.execute();
    }
}
