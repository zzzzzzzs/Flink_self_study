package com.me.study.transform;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/*
TODO process算子在Flink算是一个比较底层的算子,很多类型的流上都可以调用,可以从流中获取更多的信息(不仅仅数据本身)
* */
public class transform_12_process1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 不写分区的话就默认是电脑的核数
        env
                .fromElements("Hello world", "Hello Flink", "Hello Spark")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] ss = value.split(" ");
                        for (String s:ss) {
                            out.collect(Tuple2.of(s, 1));
                        }
                    }
                })
                .keyBy(r->r.f0)
                // TODO rich函数都可以当参数传入, 在keyBy之后的流上使用
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(Tuple2.of(value.f0, value.f1));
                    }
                })
                .print();

        env.execute();
    }
}
