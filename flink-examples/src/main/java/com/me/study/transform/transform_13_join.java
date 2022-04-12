package com.me.study.transform;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/*
TODO 基于窗口的join （和窗口的类型对应）
    https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/datastream/operators/joining/
* */
public class transform_13_join {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<Tuple2<String, Long>> stream1 =
        env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 1000L),
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 2000L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                          @Override
                          public long extractTimestamp(
                              Tuple2<String, Long> stringLongTuple2, long l) {
                            return stringLongTuple2.f1;
                          }
                        }));

    DataStream<Tuple2<String, Long>> stream2 =
        env.fromElements(
                Tuple2.of("a", 3000L),
                Tuple2.of("b", 3000L),
                Tuple2.of("a", 6000L),
                Tuple2.of("b", 4000L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                          @Override
                          public long extractTimestamp(
                              Tuple2<String, Long> stringLongTuple2, long l) {
                            return stringLongTuple2.f1;
                          }
                        }));

    stream1
        .join(stream2)
        .where(r -> r.f0)
        .equalTo(r -> r.f0)
        // Tumbling Window Join
        // Sliding Window Join
        // Session Window Join
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        // .window(SlidingEventTimeWindows.of(Time.seconds(2),Time.milliseconds(500)))
        //        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
        .apply(
            new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
              @Override
              public String join(Tuple2<String, Long> left, Tuple2<String, Long> right)
                  throws Exception {
                return left + "=>" + right;
              }
            })
        .print();

    env.execute();
  }
}
