package com.me.cep;

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
//                                  reset
//                         +------------------------+
//                         |                        |
//                         |                        |
//                         |                        |
//                         v                        |
//                     +---------+  success   +-----------+
//         +---------->+INITIAL  +----------->+SUCCESS    |
//         |           +---------+            +-----------+
//         |                |                       ^   ^
//         |           fail |                       |   |
//         |                |              success  |   |
//         |                v                       |   |
//         |           +---------+                  |   |
//         |           |   S1    +------------------+   |
//         |           +---------+                      |
//         |                |                           |
//   reset |           fail |                           |
//         |                |                    success|
//         |                v                           |
//         |           +---------+                      |
//         |           |   S2    +----------------------+
//         |           +---------+
//         |                |
//         |                |
//         |           fail |
//         |                |
//         |                v
//         |           +---------+
//         +-----------+  FAIL   |
//                     +---------+

// TODO 使用状态机实现连续三次登录失败
// TODO 如果在工作中可以将状态机画出来，可以减少大量的if else
public class NFA_UserFailedThreeConsecutiveTimes {
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
                        // TODO 初始化状态机
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

    public static class Event {
        public String userId;
        public String eventType;
        public Long timestamp;

        public Event(String userId, String eventType, Long timestamp) {
            this.userId = userId;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }

        public Event() {
        }

        @Override
        public String toString() {
            return "Event{" +
                    "userId='" + userId + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
