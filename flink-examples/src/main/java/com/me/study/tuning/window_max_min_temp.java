package com.me.study.tuning;

import java.sql.Timestamp;

import com.me.study.bean.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.me.study.source.SensorSource;

// TODO 求窗口的最大温度值和最小温度值
public class window_max_min_temp {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.addSource(new SensorSource())
        .filter(r -> r.id.equals("sensor_1"))
        .keyBy(r -> r.id)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        .aggregate(new Agg(), new WindowResult())
        .print();

    env.execute();
  }

  public static class Agg
      implements AggregateFunction<SensorReading, Tuple2<Double, Double>, Tuple2<Double, Double>> {
    @Override
    public Tuple2<Double, Double> createAccumulator() {
      // 最大值的初始值是负无穷大
      // 最小值的初始值是无穷大
      return Tuple2.of(-Double.MAX_VALUE, Double.MAX_VALUE);
    }

    @Override
    public Tuple2<Double, Double> add(SensorReading value, Tuple2<Double, Double> accumulator) {
      return Tuple2.of(
          Math.max(value.temperature, accumulator.f0), Math.min(value.temperature, accumulator.f1));
    }

    @Override
    public Tuple2<Double, Double> getResult(Tuple2<Double, Double> accumulator) {
      return accumulator;
    }

    @Override
    public Tuple2<Double, Double> merge(Tuple2<Double, Double> a, Tuple2<Double, Double> b) {
      return null;
    }
  }

  public static class WindowResult
      extends ProcessWindowFunction<
          Tuple2<Double, Double>, MaxMinTempPerWindow, String, TimeWindow> {
    @Override
    public void process(
        String s,
        Context context,
        Iterable<Tuple2<Double, Double>> iterable,
        Collector<MaxMinTempPerWindow> collector)
        throws Exception {
      Tuple2<Double, Double> tuple2 = iterable.iterator().next();
      collector.collect(
          new MaxMinTempPerWindow(
              s, tuple2.f0, tuple2.f1, context.window().getStart(), context.window().getEnd()));
    }
  }

  public static class MaxMinTempPerWindow {
    public String id;
    public Double maxTemp;
    public Double minTemp;
    public Long windowStart;
    public Long windowEnd;

    public MaxMinTempPerWindow() {}

    public MaxMinTempPerWindow(
        String id, Double maxTemp, Double minTemp, Long windowStart, Long windowEnd) {
      this.id = id;
      this.maxTemp = maxTemp;
      this.minTemp = minTemp;
      this.windowStart = windowStart;
      this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
      return "MaxMinTempPerWindow{"
          + "id='"
          + id
          + '\''
          + ", maxTemp="
          + maxTemp
          + ", minTemp="
          + minTemp
          + ", windowStart="
          + new Timestamp(windowStart)
          + ", windowEnd="
          + new Timestamp(windowEnd)
          + '}';
    }
  }
}
