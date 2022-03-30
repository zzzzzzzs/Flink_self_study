package com.me.requirement;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

// TODO 每个小时中的实时热门商品 TOP-N
public class LiveHotItemsEveryHour {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        env
                .readTextFile("./file/UserBehavior.csv")
                // ETL
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
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }))
                // 按照商品ID keyBy，下游统计每个商品ID的浏览次数
                .keyBy(r -> r.itemId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                // 聚合，因为有 ProcessWindowFunction ，所以需要处理完这1个小时的数据再向下游发送ItemViewCount
                .aggregate(new CountAgg(), new WindowResult())
                // 拿到上游发送的 ItemViewCount 然后按照窗口结束时间keyBy，这样就将每个小时分组，然后下游就可以处理每个小时的数据
                .keyBy(r -> r.windowEnd)
                // 统计每个分组下的TOP-N商品
                .process(new TopN(3))
                .print();

        env.execute();
    }

    // 使用KeyedProcessFunction 获取每个key
    public static class TopN extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private ListState<ItemViewCount> listState;
        private Integer N;

        public TopN(Integer n) {
            N = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("list", Types.POJO(ItemViewCount.class))
            );
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            // 将每次来的数据保存到状态列表中
            listState.add(itemViewCount);
            // 使用窗口结束的时间+100L（自己定义） 注册一个事件定时器，当数据的时间戳超过注册的定时器时间，则触发
            context.timerService().registerEventTimeTimer(itemViewCount.windowEnd  + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<ItemViewCount> itemViewCounts = new ArrayList<>();
            for (ItemViewCount e : listState.get()) {
                itemViewCounts.add(e);
            }
            // 清空列表状态
            listState.clear();

            // 从大到小排序
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount t1, ItemViewCount t2) {
                    return t2.count.intValue() - t1.count.intValue();
                }
            });

            StringBuilder result = new StringBuilder();
            result
                    .append("=====================================\n");
            for (int i = 0; i < N; i++) {
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                result
                        .append("窗口结束时间是：" + new Timestamp(timestamp - 100L))
                        .append("第" + (i + 1) + "名的商品id是：" + itemViewCount.itemId)
                        .append("浏览次数是：" + itemViewCount.count + "\n");
            }
            result
                    .append("=====================================\n");
            out.collect(result.toString());
        }
    }

    // 来一条数据就+1
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

    // 按照开窗的时间大小做最后的输出，目的是向下游输出窗口结束信息，用来keyBy
    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
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

    public static class UserBehavior {
        public String userId;
        public String itemId; // 商品ID
        public String categoryId;
        public String behaviorType;
        public Long timestamp;

        public UserBehavior() {
        }

        public UserBehavior(String userId, String itemId, String categoryId, String behaviorType, Long timestamp) {
            this.userId = userId;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.behaviorType = behaviorType;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userId='" + userId + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", categoryId='" + categoryId + '\'' +
                    ", behaviorType='" + behaviorType + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }
}
