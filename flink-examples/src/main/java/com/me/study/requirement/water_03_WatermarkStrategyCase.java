package com.me.study.requirement;

import com.me.study.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class water_03_WatermarkStrategyCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        /*
         TODO　需求：统计每个小时内，不同itemID浏览的个数
            map------->etl
            浏览------>filter(pv)
            每个小时--->生成水平线
            不同itemId-->keyBy
            开窗-------->一个小时
            统计------>次数
        * */
        env
                .readTextFile("./file/UserBehavior.csv")
                // 进行ETL
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        // 数据清洗
                        return new UserBehavior(
                                arr[0], arr[1], arr[2], arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                // 选出用户行为是pv的
                .filter(r -> r.behaviorType.equals("pv"))
                // TODO 必须在keyBY之前生成水平线，水平线会随着流向所有的下游
                .assignTimestampsAndWatermarks(
                        // 过期时间为0，由于是线性增长的，所以就写了一个语法糖
                        // 等价于forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        // WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .keyBy(r -> r.itemId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                // TODO 统计1个小时内事件发生
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        // 来一条数据累加1次
        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        // 将累加的数据输出
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            // 按照itemId分组，最后输出每个小时内有多少用户下单
            collector.collect(new ItemViewCount(s, iterable.iterator().next(), context.window().getStart(), context.window().getEnd()));
        }
    }

    public static class ItemViewCount {
        public String itemId;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public ItemViewCount() {
        }

        public ItemViewCount(String itemId, Long count, Long windowStart, Long windowEnd) {
            this.itemId = itemId;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "ItemViewCount{" +
                    "itemId='" + itemId + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }

}
