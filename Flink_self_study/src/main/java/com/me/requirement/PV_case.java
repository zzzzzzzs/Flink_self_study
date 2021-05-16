package com.me.requirement;

import com.me.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;


/*
    TODO 窗口开始时间 = 时间戳 - 时间戳 % 窗口长度
        窗口结束时间 = 时间戳 - 时间戳 % 窗口长度 + 窗口长度
        每个窗口的pv数据 PageView 页面浏览次数（访问量）
* */
public class PV_case {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
            TODO 需求：统计每个窗口（1个小时）的pv数
        * */
        env
                .readTextFile("./file/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0], arr[1], arr[2], arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behaviorType.equals("pv"))
                .assignTimestampsAndWatermarks(
                        // TODO 这里统计的是离线的窗口数据，就不用设置延时时间
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new CountAgg(), new PV())
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class PV extends ProcessAllWindowFunction<Long, String, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            String windowStart = new Timestamp(context.window().getStart()).toString();
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            collector.collect(windowStart + "~" + windowEnd + "的pv数据是：" + iterable.iterator().next());
        }
    }
}
