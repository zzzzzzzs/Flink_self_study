package com.me.Transform;

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
TODO
* */
public class transform_18_AggregateFunction_ProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorSource())
                .filter(r -> r.id.equals("sensor_1"))
                .keyBy(r -> r.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new Agg(), new WindowResult())
                .print();

        env.execute();
    }

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

    public static class Agg implements AggregateFunction<SensorReading, Tuple2<Double, Long>, Double> {
        @Override
        public Tuple2<Double, Long> createAccumulator() {
            return Tuple2.of(0.0, 0L);
        }

        @Override
        public Tuple2<Double, Long> add(SensorReading value, Tuple2<Double, Long> accumulator) {
            return Tuple2.of(accumulator.f0 + value.temperature, accumulator.f1 + 1L);
        }

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
            collector.collect(new Avg(key, iterable.iterator().next(), context.window().getStart(), context.window().getEnd()));
        }
    }
}
