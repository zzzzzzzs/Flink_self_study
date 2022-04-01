package com.me.transform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// TODO 简单的聚合操作
public class transform_easy {
  public static void main(String[] args) throws Exception {
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Configuration conf = new Configuration();
    conf.setString(RestOptions.BIND_PORT, "8081-8089");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
    env.setParallelism(1);
    DataStreamSource<Tuple2<String, Integer>> stream =
      env.fromElements(Tuple2.of("a", 1), Tuple2.of("a", 3), Tuple2.of("b", 3), Tuple2.of("b", 4));
    stream.keyBy(r -> r.f0).sum(1).print();
    stream.keyBy(r -> r.f0).sum("f1").print();
    stream.keyBy(r -> r.f0).max(1).print();
    stream.keyBy(r -> r.f0).max("f1").print();
    stream.keyBy(r -> r.f0).min(1).print();
    stream.keyBy(r -> r.f0).min("f1").print();
    stream.keyBy(r -> r.f0).maxBy(1).print();
    stream.keyBy(r -> r.f0).maxBy("f1").print();
    stream.keyBy(r -> r.f0).minBy(1).print();
    stream.keyBy(r -> r.f0).minBy("f1").print();
    env.execute();
  }
}
