package com.me.Transform;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//
/*
TODO DataStream API 提供的所有转换操作函数都拥有它们的富版本
     富版本拥有生命周期的概念
    RichMapFunction：
* */
public class transform_11_RichMapFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 不写分区的话就默认是电脑的核数
        env
                .fromElements(1, 2, 3, 4)
                .map(new RichMapFunction<Integer, Integer>() {
                    // 做一些初始化工作
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("enter lifecycle");
                    }

                    // 做具体的业务操作
                    @Override
                    public Integer map(Integer value) throws Exception {
                        System.out.println(getRuntimeContext().getIndexOfThisSubtask());
                        return value + 1;
                    }

                    // 清理工作，断开和 HDFS 的连接。
                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("exit lifecycle");
                    }
                })
                .print();

        env.execute();
    }
}