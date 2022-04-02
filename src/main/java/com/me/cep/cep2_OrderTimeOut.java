package com.me.cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

// TODO 使用cep实现订单超时的检测
public class cep2_OrderTimeOut {
    private static OutputTag<String> timeoutOrder = new OutputTag<String>("timeout-order") {
    };

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

        // TODO 定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("create")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .next("pay")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                // TODO 要求两个事件紧挨着，要在5s之内发生
                .within(Time.seconds(5));

        PatternStream<Event> patternStream = CEP.pattern(orderStream.keyBy(r -> r.orderId), pattern);

        SingleOutputStreamOperator<String> result = patternStream
                .flatSelect(
                        // TODO 将超时的信息输出到的侧输出标签
                        timeoutOrder,
                        // TODO 处理超时的事件
                        new PatternFlatTimeoutFunction<Event, String>() {
                            @Override
                            public void timeout(Map<String, List<Event>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                                Event create = pattern.get("create").get(0);
                                // 将数据发送到了侧输出流
                                out.collect("订单" + create.orderId + "支付超时");
                            }
                        },
                        // TODO 处理正常的事件
                        new PatternFlatSelectFunction<Event, String>() {
                            @Override
                            public void flatSelect(Map<String, List<Event>> pattern, Collector<String> out) throws Exception {
                                Event pay = pattern.get("pay").get(0);
                                // 将数据直接发送到下游
                                out.collect("订单" + pay.orderId + "支付成功");
                            }
                        }
                );

        // 主流打印正常的事件
        result.print();
        // 在侧输出流中打印超时的事件
        result.getSideOutput(timeoutOrder).print();

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
