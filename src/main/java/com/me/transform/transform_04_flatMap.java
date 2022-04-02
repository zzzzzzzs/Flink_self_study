package com.me.transform;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// TODO flatMap语义：将列表中的每一个元素转化成0个，1个或者多个元素
public class transform_04_flatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO white输出，black输出两次，gray不输出
        DataStreamSource<String> stream = env.fromElements("white", "black", "gray");

        // 在Java中推荐使用这种方法
        stream
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        if (value.equals("white")) {
                            out.collect(value);
                        } else if (value.equals("black")) {
                            out.collect(value);
                            out.collect(value);
                        }
                    }
                })
                .print();

        stream
                .flatMap(new MyFlatMap())
                .print();

        stream
                // 匿名函数实现方式，可读性不强
                /*
                flatMap() 这样的函数，它的签名 void flatMap(IN value, Collector<OUT>out)
                被 Java 编译器编译为 void flatMap(IN value, Collector out)。这样 Flink 就无法自动推断输出的类型信息了。
                 */
                .flatMap((String value, Collector<String> out) -> {
                    if (value.equals("white")) {
                        out.collect(value);
                    } else if (value.equals("black")) {
                        out.collect(value);
                        out.collect(value);
                    }
                })
                // 告诉Flink，flatMap输出的类型是String，因为Java 8推断不出flatMap的输出类型
                .returns(Types.STRING)
                .print();

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (value.equals("white")) {
                out.collect(value);
            } else if (value.equals("black")) {
                out.collect(value);
                out.collect(value);
            }
        }
    }
}
