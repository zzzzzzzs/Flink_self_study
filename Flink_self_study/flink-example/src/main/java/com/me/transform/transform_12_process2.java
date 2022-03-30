package com.me.transform;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/*
TODO process算子在Flink算是一个比较底层的算子,很多类型的流上都可以调用,可以从流中获取更多的信息(不仅仅数据本身)，
    process可以向下游发送多次
* */
public class transform_12_process2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 不写分区的话就默认是电脑的核数
        env
                .fromElements(1, 2, 3, 4, 1)
                // TODO 由于是分布式的， keyBy会将数据逻辑分区，不同的数据对应不同的 process算子，此时状态也是不一样的。如果想让状态一样可以将keyBy中的参数设置成一样的
                .keyBy(r -> 1)
                // TODO rich函数都可以当参数传入, 在keyBy之前的流上使用
                .process(new ProcessFunction<Integer, Integer>() {
                    // TODO 这里在 process 中使用了状态变量就必须有 keyBy 操作。如果没有keyBy在有限流中会报错
                    private ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("------");
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", Integer.class));
                    }

                    // TODO 数据来一条处理一条
                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                        state.update(state.value() != null ? state.value() + 1 : 0);
                        System.out.println("此时计数器为：" + state.value());
                        out.collect(value);
                    }
                })
                .print();

        env.execute();
    }
}
