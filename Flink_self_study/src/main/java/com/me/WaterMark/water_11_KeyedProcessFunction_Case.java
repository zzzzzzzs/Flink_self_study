package com.me.WaterMark;

import com.me.Transform.SensorReading;
import com.me.Transform.SensorSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;



/*
    TODO 检测传感器的连续1s温度上升
* */

public class water_11_KeyedProcessFunction_Case {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
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
            // TODO　在这里初始化状态变量
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE));
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-Ts", Types.LONG));
        }

        // 每来一条数据就触发一次
        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            double prevTemp = 0.0; // 最近一次温度初始化
            if (lastTemp.value() == null) {
                // 当第一条数据到来时，状态变量为空
                lastTemp.update(value.temperature);
            } else {
                prevTemp = lastTemp.value();
                lastTemp.update(value.temperature);
            }
            long ts = 0L;
            if (timerTs.value() == null) {

            } else {
                ts = timerTs.value();
            }

            if (prevTemp == 0.0 || value.temperature < prevTemp) {
                // 如果没有对应的定时器存在，语句不执行
                ctx.timerService().deleteEventTimeTimer(ts);
                timerTs.clear();
            } else if (value.temperature > prevTemp && ts == 0L) {
                ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + 1000L);
                timerTs.update(ctx.timerService().currentProcessingTime() + 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("连续1s温度上升了！传感器是：" + ctx.getCurrentKey());
            timerTs.clear();
        }
    }
}
