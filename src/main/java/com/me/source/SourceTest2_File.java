package com.me.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 从文件读取数据 一般是测试，或者离线处理
        DataStream<String> dataStream = env.readTextFile("./file/UserBehavior.csv");

        // 打印输出
        dataStream.print();

        env.execute();
    }
}
