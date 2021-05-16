package com.me.cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// TODO 使用底层API实现订单超时
// TODO 使用了大量的if，不用看
public class BasicAPIImplOrderTimeout {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> orderStream = env
                .fromElements(
                        new Event("order-1", "create", 1000L),
                        new Event("order-2", "create", 2000L),
                        new Event("order-1", "pay", 4000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        orderStream
                .keyBy(r -> r.orderId)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private transient ValueState<Event> state;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        state = getRuntimeContext().getState(
                                new ValueStateDescriptor<Event>("event", Types.POJO(Event.class))
                        );
                    }

                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        if (value.eventType.equals("create")) {
                            if (state.value() != null && state.value().eventType.equals("pay")) {
                                out.collect("订单" + value.orderId + "支付成功");
                            } else {
                                state.update(value);
                                ctx.timerService().registerEventTimeTimer(value.timestamp + 5000L);
                            }
                        } else if (value.eventType.equals("pay")) {
                            state.update(value);
                            out.collect("订单" + value.orderId + "支付成功");
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if (state.value() != null && state.value().eventType.equals("create")) {
                            out.collect("订单" + state.value().orderId + "支付超时");
                            state.clear();
                        }
                    }
                })
                .print();

        env.execute();
    }

    public static class Event {
        public String orderId;
        public String eventType;
        public Long timestamp;

        public Event() {
        }

        public Event(String orderId, String eventType, Long timestamp) {
            this.orderId = orderId;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "orderId='" + orderId + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
