package com.me.transform;

import com.me.bean.SensorReading;
import com.me.source.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// TODO 使用reduce实现求平均值的操作 求整条流的温度平均值
public class transform_07_reduce_avg {
  public static void main(String[] args) throws Exception {
    // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Configuration conf = new Configuration();
    conf.setString(RestOptions.BIND_PORT, "8081-8089");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
    env.setParallelism(1);

    env.addSource(new SensorSource()).map(new MapFunction<SensorReading, Tuple3<String, Double, Long>>() {
      @Override
      public Tuple3<String, Double, Long> map(SensorReading value) throws Exception {
        return Tuple3.of(value.id, value.temperature, 1L);
      }
    }).keyBy(r -> r.f0).reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
      @Override
      public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> value1,
        Tuple3<String, Double, Long> value2) throws Exception {
        return Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
      }
    }).map(new MapFunction<Tuple3<String, Double, Long>, Tuple2<String, Double>>() {
      @Override
      public Tuple2<String, Double> map(Tuple3<String, Double, Long> value) throws Exception {
        return Tuple2.of(value.f0, value.f1 / value.f2);
      }
    }).print();

    env.execute();
  }
}
