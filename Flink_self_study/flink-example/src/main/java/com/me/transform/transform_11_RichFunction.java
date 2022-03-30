package com.me.transform;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//
/*
TODO DataStream API 提供的所有转换操作函数都拥有它们的Rich版本。
        它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。
    RichMapFunction：
    RichFlatMapFunction
    RichFilterFunction
    。。。。

* */
public class transform_11_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 不写分区的话就默认是电脑的核数
        env
                .fromElements(1, 2, 3, 4)
                .map(new RichMapFunction<Integer, Integer>() {
                    // TODO 初始化方法，当一个算子例如 map 或者 filter 被调用之前 open()会被调用。如数据库初始化连接
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // TODO 打印4次，因为有4个分区，这个是电脑默认的核数。有几个分区，map实例化几次
                        System.out.println("enter lifecycle");
                    }

                    // TODO 做具体的业务操作
                    @Override
                    public Integer map(Integer value) throws Exception {
                        // TODO getRuntimeContext()方法提供了函数的 RuntimeContext 的一些信息，
                        //  例如函数执行的并行度，任务的名字，以及 state 状态
                        //  getRuntimeContext等方法是父类的方法，可以直接调用
                        System.out.println(getRuntimeContext().getIndexOfThisSubtask());
                        return value + 1;
                    }

                    // TODO 是生命周期中的最后一个调用的方法，做一些清理工作。如 断开和 HDFS 的连接。
                    @Override
                    public void close() throws Exception {
                        super.close();
                        // TODO 打印4次，因为有4个分区，这个是电脑默认的核数。有几个分区，map实例化几次
                        System.out.println("exit lifecycle");
                    }
                })
                .print();

        env.execute();
    }
}
