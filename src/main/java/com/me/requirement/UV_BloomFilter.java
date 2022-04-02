package com.me.requirement;


import com.me.bean.UserBehavior;
import org.apache.commons.codec.Charsets;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.hash.BloomFilter;
import org.apache.flink.calcite.shaded.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// TODO 计算uv,使用bloom filter
public class UV_BloomFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("./file/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
                        String[] arr = value.split(",");
                        out.collect(new UserBehavior(
                                arr[0],arr[1],arr[2],arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        ));
                    }
                })
                .filter(r -> r.behaviorType.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }))
                .keyBy(r -> true)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Tuple2<BloomFilter<String>, Long>, Long> {
        @Override
        public Tuple2<BloomFilter<String>, Long> createAccumulator() {
            // 用户数量约为10万级别，误判率为0.01
            // 这里需要用布隆计算器计算 https://hur.st/bloomfilter/
            return Tuple2.of(BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 100000, 0.0001), 0L);
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> add(UserBehavior value, Tuple2<BloomFilter<String>, Long> accumulator) {
            if (!accumulator.f0.mightContain(value.userId)) {
                accumulator.f0.put(value.userId);
                accumulator.f1 += 1L;
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<String>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> merge(Tuple2<BloomFilter<String>, Long> a, Tuple2<BloomFilter<String>, Long> b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean key, Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            collector.collect(
                    "窗口" + new Timestamp(context.window().getStart())
                            + "~~" + new Timestamp(context.window().getEnd())
                            + "的uv数据是：" + iterable.iterator().next()
            );
        }
    }
}

