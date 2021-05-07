package com.me.cep;

import com.me.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class cep1 {
    private static HashMap<Tuple2<String, String>, String> statemachine = new HashMap<>();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env
                .fromElements(
                        new Event("user-1", "fail", 1000L),
                        new Event("user-1", "fail", 2000L),
                        new Event("user-1", "fail", 5000L),
                        new Event("user-2", "success", 10000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        stream
                .keyBy(r -> r.userId)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        state = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("state", Types.STRING)
                        );

                        statemachine.put(Tuple2.of("initial", "fail"), "S1");
                        statemachine.put(Tuple2.of("initial", "success"), "SUCCESS");
                        statemachine.put(Tuple2.of("S1", "fail"), "S2");
                        statemachine.put(Tuple2.of("S2", "fail"), "FAIL");
                        statemachine.put(Tuple2.of("S1", "success"), "SUCCESS");
                        statemachine.put(Tuple2.of("S2", "success"), "SUCCESS");
                    }

                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        if (state.value() == null) {
                            state.update("initial");
                        }
                        String nextState = statemachine.get(Tuple2.of(state.value(), value.eventType));
                        if (nextState.equals("SUCCESS")) {
                            out.collect("用户" + value.userId + "登录成功");
                            state.update("initial");
                        } else if (nextState.equals("FAIL")) {
                            out.collect("用户" + value.userId + "登录失败");
                            state.update("initial");
                        } else {
                            state.update(nextState);
                        }
                    }
                })
                .print();

        env.execute();
    }
}
