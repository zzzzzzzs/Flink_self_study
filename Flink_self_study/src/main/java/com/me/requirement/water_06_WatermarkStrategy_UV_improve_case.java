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
import java.util.HashSet;


/*
    TODO 窗口开始时间 = 时间戳 - 时间戳 % 窗口长度
        窗口结束时间 = 时间戳 - 时间戳 % 窗口长度 + 窗口长度
        每个窗口的uv数据 Unique Visitor 独立访客 使用pv去重以后就是uv
        　　查询一个小时之内的用户访问量（一个用户算一个）

难点：如果用户量很多，要想用Set等数据结构实现去重不太现实，随时都会OOM，这时就得利用布隆过滤器，先判断user是否存在，不存在则计数+1，存在则不做计算，这样能节省大量的内存空间
* */
public class water_06_WatermarkStrategy_UV_improve_case {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
            TODO 需求：统计每个窗口（1个小时）的uv数，需要使用userID去重
                使用HashSet的幂等性
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
                .aggregate(new CountAgg(), new UV())
                .print();

        env.execute();
    }


//    // TODO 重点：使用HashSet将userId去重
    public static class CountAgg implements AggregateFunction<UserBehavior, HashSet<String>, Long> {


    @Override
    public HashSet<String> createAccumulator() {
        return null;
    }

    @Override
    public HashSet<String> add(UserBehavior value, HashSet<String> accumulator) {
        return null;
    }

    @Override
    public Long getResult(HashSet<String> accumulator) {
        return null;
    }

    @Override
    public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
        return null;
    }
}

    public static class UV extends ProcessAllWindowFunction<Long, String, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            String windowStart = new Timestamp(context.window().getStart()).toString();
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            collector.collect(windowStart + "~" + windowEnd + "的uv数据是：" + iterable.iterator().next());
        }
    }
}
