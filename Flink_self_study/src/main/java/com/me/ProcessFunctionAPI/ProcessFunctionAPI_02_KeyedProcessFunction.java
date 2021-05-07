package com.me.ProcessFunctionAPI;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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

public class ProcessFunctionAPI_02_KeyedProcessFunction {
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
                    ValueState<Long> tsTimerState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        tsTimerState =  getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
                    }

                    //  TODO 每来一条数据，调用一次
                    @Override
                    public void processElement(String value, Context context, Collector<String> collector) throws Exception {
                        long currTs = context.timerService().currentProcessingTime();
                        collector.collect("数据到达，到达时间是：" + new Timestamp(currTs) + "数据为：" + value);

                        // Context
                        // 获取当前时间戳
                        context.timestamp();
                        // 获取key
                        context.getCurrentKey();
                        // 侧输出流
//                        context.output();

                        // 如果是处理事件可以获取当前的处理时间
                        context.timerService().currentProcessingTime();
                        // 如果是事件时间可以获取到水位线
                        context.timerService().currentWatermark();
                        // 注册10s之后的定时器，定时器是onTimer  处理时间定时器
                        context.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                        // 事件时间定时器
//                        context.timerService().registerEventTimeTimer(1000L);
                        tsTimerState.update(context.timerService().currentProcessingTime() + 1000L);
                        // 删除定时器，先使用状态变量保存时间戳，然后delete
                        //            ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value());
                    }

                    // 当到达注册时间后调用
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("定时器触发了，触发时间是：" + new Timestamp(timestamp));
                        // 时间域，只能判断事件时间，还是处理时间，功能少
                        ctx.timeDomain();
                        // 获取当前Key
                        ctx.getCurrentKey();
                    }
                })
                .print();

        env.execute();
    }
}
