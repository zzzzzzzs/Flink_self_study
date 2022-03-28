package com.me.WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountExample {
  // 不要忘记抛出异常
  public static void main(String[] args) throws Exception {
    // 获取执行环境上下文
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 设置并行任务的数量
    env.setParallelism(1);

    env.fromElements("Hello World", "Hello World")
        .flatMap(
            new FlatMapFunction<String, WordCountFromBatch.WordWithCount>() {
              @Override
              public void flatMap(String value, Collector<WordCountFromBatch.WordWithCount> out)
                  throws Exception {
                // 使用空格进行切分
                String[] arr = value.split(" ");
                // 使用collect方法向下游发送数据
                for (String e : arr) {
                  out.collect(new WordCountFromBatch.WordWithCount(e, 1L));
                }
              }
            })
        .keyBy(r -> r.word)
        .print();

    // 不要忘记执行
    env.execute();
  }

  // POJO Class
  // 类必须是公有类
  // 所有字段必须是public
  // 必须有空构造器
  public static class WordWithCount {
    public String word;
    public Long count;

    public WordWithCount() {}

    public WordWithCount(String word, Long count) {
      this.word = word;
      this.count = count;
    }

    @Override
    public String toString() {
      return "WordWithCount{" + "word='" + word + '\'' + ", count=" + count + '}';
    }
  }
}
