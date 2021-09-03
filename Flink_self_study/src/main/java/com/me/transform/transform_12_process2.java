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
                .fromElements(1, 2, 3, 4)
                .keyBy(r->r.intValue())
                // TODO rich函数都可以当参数传入, 在keyBy之前的流上使用
                .process(new ProcessFunction<Integer, Integer>() {
                    // TODO 这里在 process 中使用了状态变量就必须有 keyBy 操作。如果没有keyBy在有限流中会报错
                    private ValueState<String> state;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("------");
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", String.class));
                    }
                    // TODO 数据来一条处理一条
                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                        state.update("a");
                        out.collect(value);
                    }
                })
                .print();

        env.execute();
    }
}
