package com.me.WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountFromSocket1 {
  // 不要忘记抛出异常
  public static void main(String[] args) throws Exception {
    // 获取执行环境上下文
    // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Configuration conf = new Configuration();
    conf.setString(RestOptions.BIND_PORT, "8081-8089");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
    // 设置并行任务的数量
    env.setParallelism(1);

    // 数据源
    // 先启动 nc -lk 9999
    // windows运行：nc -lp 9999
    DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

    // 基于数据流进行转换计算
    stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
      @Override
      public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] ss = value.split(" ");
        for (String s : ss) {
          out.collect(Tuple2.of(s, 1));
        }
      }
    }).keyBy(r -> r.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
      @Override
      public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2)
        throws Exception {
        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
      }
    }).print().setParallelism(1);

    // 执行任务
    env.execute();
  }

}
