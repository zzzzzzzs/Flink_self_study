package com.me.transform;

import com.me.bean.SensorReading;
import com.me.source.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;


/*
    TODO  ReduceFunction/AggregateFunction 和 ProcessWindowFunction 结合起来使用
        ReduceFunction/AggregateFunction 做增量聚合，ProcessWindowFunction 提供更多的对数据流的访问权限。
        如果只使用 ProcessWindowFunction(底层的实现为将事件都保存在 ListState 中)，将会非常占用空间。
        结合使用的话分配到某个窗口的元素将被提前聚合，而当窗口的trigger 触发时，也就是窗口收集完数据关闭时，
        将会把聚合结果发送到 ProcessWindow-Function 中，这时 Iterable 参数将会只有一个值，就是前面聚合的值。

        TODO 尽量使用增量聚合&全窗口聚合一起使用，先做预聚合
* */
public class transform_18_AggregateFunction_ProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        env
                .addSource(new SensorSource())
//                .filter(r -> r.id.equals("sensor_1"))
                .keyBy(r -> r.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 将增量聚合函数最终结果返回给ProcessWindowFunction，这样就会有更多的访问权限
                .aggregate(new Agg(), new WindowResult())
                .print();

        env.execute();
    }



    public static class Agg implements AggregateFunction<SensorReading, Tuple2<Double, Long>, Double> {
        @Override
        public Tuple2<Double, Long> createAccumulator() {
            return Tuple2.of(0.0, 0L);
        }

        @Override
        public Tuple2<Double, Long> add(SensorReading value, Tuple2<Double, Long> accumulator) {
            return Tuple2.of(accumulator.f0 + value.temperature, accumulator.f1 + 1L);
        }
        // 直接输出结果
        @Override
        public Double getResult(Tuple2<Double, Long> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
            return null;
        }
    }



    public static class WindowResult extends ProcessWindowFunction<Double, Avg, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Double> iterable, Collector<Avg> collector) throws Exception {
            System.out.println("---------------------------------");
            collector.collect(new Avg(key, iterable.iterator().next(), context.window().getStart(), context.window().getEnd()));
        }
    }


    // 全窗口的输出
    public static class Avg {
        public String id;
        public Double avg;
        public Long windowStart;
        public Long windowEnd;

        public Avg() {
        }

        public Avg(String id, Double avg, Long windowStart, Long windowEnd) {
            this.id = id;
            this.avg = avg;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "Avg{" +
                    "id='" + id + '\'' +
                    ", avg=" + avg +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }
}
