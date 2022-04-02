package com.me.study.cep;

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

// TODO 匹配用户连续三次登录失败
public class cep3_UserFailedThreeConsecutiveTimes {
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

        // 定义模板
        // a{3}，减少冗余字段，因为使用的时times(3)，因此后面的select中的参数使用的是List
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("fail") // 第一个事件取一个名字
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                })
                // 连续3个事件
                .times(3);

        // 在流上查找符合模板的数据
        PatternStream<Event> patternStream = CEP.pattern(stream.keyBy(r -> r.userId), pattern);

        // 输出匹配到的数据
        SingleOutputStreamOperator<String> result = patternStream
                .select(new PatternSelectFunction<Event, String>() {
                    @Override
                    public String select(Map<String, List<Event>> map) throws Exception {
                        // TODO 因为上面Pattern使用的是times(3)，减少了冗余字段
                        Event first = map.get("fail").get(0);
                        Event second = map.get("fail").get(1);
                        Event third = map.get("fail").get(2);
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
