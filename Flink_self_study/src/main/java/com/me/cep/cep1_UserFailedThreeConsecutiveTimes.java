package com.me.cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

// TODO 匹配 用户连续三次登录失败
public class cep1_UserFailedThreeConsecutiveTimes {
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

        // 定义CEP模板
        Pattern<Event, Event> pattern = Pattern
                // 第一个事件取一个名字
                .<Event>begin("first")
                // 筛选条件
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                // next的是意思是：紧挨着第一个事件，取一个名字
                .next("second")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                .next("third")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                });

        // 在每一个组上进行匹配
        PatternStream<Event> patternStream = CEP.pattern(stream.keyBy(r -> r.userId), pattern);

        // 输出匹配到的数据
        SingleOutputStreamOperator<String> result = patternStream
                .select(new PatternSelectFunction<Event, String>() {
                    @Override
                    public String select(Map<String, List<Event>> map) throws Exception {
                        // first => List<Event("fail")>
                        // second => List<Event("fail")>
                        // third => List<Event("fail")>
                        Event first = map.get("first").get(0);
                        Event second = map.get("second").get(0);
                        Event third = map.get("third").get(0);
                        return "用户" + first.userId + "在" + first.timestamp + "," + second.timestamp + "," + third.timestamp + "时间登录失败！";
                    }
                });

        result.print();


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

