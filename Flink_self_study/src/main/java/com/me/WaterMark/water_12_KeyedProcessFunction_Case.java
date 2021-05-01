package com.me.WaterMark;

import com.me.Transform.SensorReading;
import com.me.Transform.SensorSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;



/*
    TODO 检测传感器的连续1s温度上升
* */

public class water_12_KeyedProcessFunction_Case {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorSource())
                .filter(r -> "sensor_1".equals(r.id))
                .keyBy(r -> r.id)
                .process(new KeyedProcessFunction<String, SensorReading, String>() {
                    private ListState<SensorReading> listState;
                    private ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState = getRuntimeContext().getListState(
                                new ListStateDescriptor<SensorReading>("list", Types.POJO(SensorReading.class))
                        );
                        timerTs = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("timer", Types.LONG)
                        );
                    }

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
                        listState.add(value);
                        if(timerTs.value() == null){
                            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime());
                            timerTs.update(ctx.timerService().currentProcessingTime() + 10 * 1000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("共 " + listState.get().spliterator().getExactSizeIfKnown() + " 条数据");
                        timerTs.clear();
                    }
                })
                .print();

        env.execute();
    }
}
