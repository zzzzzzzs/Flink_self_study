package com.me.WaterMark;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;


/*
    TODO KeyedProcessFunction 用来操作 KeyedStream。 以后经常使用
     KeyedProcessFunction 会处理流的每一个元素，输出为 0 个、1 个或者多个元素。
     所有的 Process Function 都继承自 RichFunction 接口，所以都有 open()、close() 和 getRuntimeContext() 等方法。
        数据在同一时间内只会触发一个定时器，一个时间戳只能注册一个时间
        KeyedProcessFunction 可以做全部的事情
    KeyedProcessFunction 要结合状态变量才可以发挥全部的威力
*
* */

public class water_07_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
        TODO　按照processing time（机器时间）
        * */

        env
                .socketTextStream("localhost", 9999)
                .keyBy(r -> true)
                // TODO KeyedProcessFunction 可以做全部的事情
                .process(new KeyedProcessFunction<Boolean, String, String>() {
                    //  TODO 每来一条数据，调用一次
                    @Override
                    public void processElement(String value, Context context, Collector<String> collector) throws Exception {
                        long currTs = context.timerService().currentProcessingTime();
                        collector.collect("数据到达，到达时间是：" + new Timestamp(currTs) + "数据为：" + value);
                        // 注册10s之后的定时器，定时器是onTimer
                        context.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                    }

                    // 当到达注册时间后调用
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("定时器触发了，触发时间是：" + new Timestamp(timestamp));
                    }
                })
                .print();

        env.execute();
    }
}
