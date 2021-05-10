package com.me.source;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.stream.Collector;

public class SourceTest3_socket {
    public static void main(String[] args) throws Exception {
        // 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                // 指定socket
                .socketTextStream("bigdata102", 9999)
                // 打印
                .print();
        // 执行
        env.execute();
    }
}
