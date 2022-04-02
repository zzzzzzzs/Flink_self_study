package com.me.study.requirement;

import com.me.study.bean.SensorReading;
import com.me.study.source.SensorSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.MessageFormat;



/*
    TODO 检测传感器的连续1s温度上升，
        1,2,3,4,5,1,2
        当第一个到达的数据会注册一个 1s 的定时器，然后在这 1s 内检测温度是否连续上升
* */

public class DetectSensorTempRise1sContinuously {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                // 1s生产10条数据
                .addSource(new SensorSource())
                .keyBy(r -> r.id)
                .process(new TempIncrease())
                .print();
        env.execute();
    }

    public static class TempIncrease extends KeyedProcessFunction<String, SensorReading, String> {
        /*
         TODO 每一个key维护自己的状态变量
                状态变量是一个单例，只会初始化一次
                状态变量仅可以用于KeyedStream
        * */
        // 声明一个状态变量，用来保存最近一次的温度值
        private ValueState<Double> lastTemp;
        // 声明一个状态变量，用来保存报警定时器的时间戳
        private ValueState<Long> timerTs;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // TODO　在这里初始化状态变量，状态变量有可能保存在hdfs或者文件系统里面，所以用这样的方式定义
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE));
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-Ts", Types.LONG));
        }

        // 每来一条数据就触发一次
        @Override
        public void processElement(SensorReading sensorReading, Context ctx, Collector<String> out) throws Exception {
            double prevTemp = 0.0; // 最近一次温度初始化
            if (lastTemp.value() == null) {
                // 当第一条数据到来时，状态变量为空
                lastTemp.update(sensorReading.temperature);
            } else {
                prevTemp = lastTemp.value();
                lastTemp.update(sensorReading.temperature);
            }
            long ts = 0L;
            if (timerTs.value() == null) {

            } else {
                ts = timerTs.value();
            }
            // 初始温度等于0 或者 温度下降
            if (prevTemp == 0.0 || sensorReading.temperature < prevTemp) {
                // 如果没有对应的定时器存在，语句不执行
                ctx.timerService().deleteEventTimeTimer(ts);
                // 将报警定时器时间戳的状态变量清空
                timerTs.clear();
                // 温度上升 且 不存在报警定时器
            } else if (sensorReading.temperature > prevTemp && ts == 0L) {
                // 注册报警定时器 = 当前时间 + 1s
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L);
                // 将定时器时间戳保存到状态变量中
                timerTs.update(ctx.timerService().currentProcessingTime() + 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            String ss =  MessageFormat.format("连续1s温度上升了！传感器是：{0} 当前时间是：{1}", ctx.getCurrentKey(), new Timestamp(ctx.timerService().currentProcessingTime()));
            out.collect(ss);
            timerTs.clear();
        }
    }
}
