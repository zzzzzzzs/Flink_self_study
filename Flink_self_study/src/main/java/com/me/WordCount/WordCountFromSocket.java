package com.me.WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountFromSocket {
    // 不要忘记抛出异常
    public static void main(String[] args) throws Exception {
        // 获取执行环境上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行任务的数量
        env.setParallelism(1);

        // 数据源
        // 先启动nc -lk 9999
        // windows运行：nc -lp 9999
        DataStreamSource<String> stream = env
                .socketTextStream("localhost", 9999);

        // map操作：string => (string, 1)
        // flatMap的语义：将列表中的每一个元素转化成0个，1个或者多个元素
        SingleOutputStreamOperator<WordWithCount> mappedStream = stream
                // 匿名类
                // 第一个泛型是输入类型
                // 第二个泛型是输出类型
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        // 使用空格进行切分
                        String[] arr = value.split(" ");
                        // 使用collect方法向下游发送数据
                        for (String e : arr) {
                            out.collect(new WordWithCount(e, 1L));
                        }
                    }
                });

        // shuffle操作
        KeyedStream<WordWithCount, String> keyedStream = mappedStream
                // 第一个泛型是流中数据的类型
                // 第二个泛型是key的类型
                .keyBy(new KeySelector<WordWithCount, String>() {
                    @Override
                    public String getKey(WordWithCount value) throws Exception {
                        return value.word;
                    }
                });

        // reduce操作
        // 累加器的类型和元素类型是一样的
        SingleOutputStreamOperator<WordWithCount> reducedStream = keyedStream
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                        return new WordWithCount(value1.word, value1.count + value2.count);
                    }
                });

        // 输出聚合的累加器结果
        reducedStream.print();

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

        public WordWithCount() {
        }

        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
