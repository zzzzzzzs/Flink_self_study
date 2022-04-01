package com.me.transform;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/*
TODO process算子在Flink算是一个比较底层的算子,很多类型的流上都可以调用,可以从流中获取更多的信息(不仅仅数据本身)，
    process可以向下游发送多次
* */
public class transform_12_process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 不写分区的话就默认是电脑的核数
        env
                .fromElements(1, 2, 3, 4)
                // TODO rich函数都可以当参数传入, 在keyBy之前的流上使用
                .process(new ProcessFunction<Integer, Integer>() {
                    // TODO 数据来一条处理一条
                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                        out.collect(value);
                    }
                })
                .print();

        env.execute();
    }
}
