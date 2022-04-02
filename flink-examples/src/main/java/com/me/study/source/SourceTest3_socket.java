package com.me.study.source;


import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class SourceTest3_socket {
    public static void main(String[] args) throws Exception {
        // 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env
                // 指定socket
                .socketTextStream("localhost", 9999)
                .keyBy(new KeySelector<String, Boolean>() {
                    @Override
                    public Boolean getKey(String value) throws Exception {
                        return true;
                    }
                })
                // 打印
                .print();
        // 执行
        env.execute();
    }
}
