package com.me.transform;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/*
TODO 分布式转化算子
    shuffle：
    rebalance：
    rescale：
* */
public class transform_10_shuffle_rebalance_rescale {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4).setParallelism(1);

        // 方法将数据随机的分配到下游算子的并行任务中去。
        stream
                .shuffle()
                .print()
                .setParallelism(2);
        // 负载均衡算法将输入流平均分配到随后的并行运行的任务中去。此方法上游的每个任务会与下游所有的任务建立通道。
        stream
                .rebalance()
                .print()
                .setParallelism(2);
        //此方法只会将数据发送到接下来的并行运行的任务中的一部分任务中。本质上，当发送者任务数量和接收者任务数量不一样时，rescale
        //分区策略提供了一种轻量级的负载均衡策略。如果接收者任务的数量是发送者任务的数量的倍数时，rescale 操作将会效率更高。
        // 此方法不会与下游所有的任务建立通道。节省IO
        stream
                .rescale()
                .print()
                .setParallelism(2);

        env.execute();
    }
}
