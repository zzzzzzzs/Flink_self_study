package com.me.bug;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Bug_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Linux nc -lk 9999 windows nc -lp 9999
//        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> stream = env.fromElements("Hello world", "Hello Flink", "Hello Spark");


        /*
            TODO 感觉这里有bug ，keyBy函数走2次
                看起来先走一遍keyBy，然后再走一遍keyBy
                目前看起来，有界流会走2变keyBy
        * */
        stream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] strings = value.split(" ");
                        for (int i = 0; i < strings.length; i++) {
                            out.collect(Tuple2.of(strings[i], 1L));
                        }
                    }
                })
                .keyBy(r -> {
                    System.out.println("------------------------" + r.f0);
                    return r.f0;
                })
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                            }
                        }
                )
                .print();
        // 不要忘记执行
        env.execute();
    }
}
