package com.me.other;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
 * TODO 全局禁止 OperatorChaining ，这样 flink webUI 显示的 DAG 显示的就是算子的个数
 */
public class WhatIsTaskNotUseOperatorChain {
  // 不要忘记抛出异常
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.setString(RestOptions.BIND_PORT, "8081-8089");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.disableOperatorChaining();
    // 设置并行任务的数量
    // env.setParallelism(1);

    // 数据源
    // 先启动 nc -lk 9999
    // windows运行：nc -lp 9999
    DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

    // 基于数据流进行转换计算
    stream.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public void flatMap(String value, Collector<String> out) {
        Arrays.stream(value.split(" ")).forEach(out::collect);
      }
    }).returns(Types.STRING).map(word -> Tuple2.of(word, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
      .keyBy(r -> r.f0).sum(1).print();

    // 执行任务
    env.execute();
  }
}
