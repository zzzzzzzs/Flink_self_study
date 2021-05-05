package com.me.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class aaa {
    public static void main(String[] args) throws Exception {
        //获得环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(2);
        //获得数据来源

        DataStreamSource<Integer> streamSource2 = env.fromCollection(Arrays.asList(21, 22, 23, 24, 25, 26, 27, 28, 29));
        DataStreamSource<Integer> streamSource1 = env.fromCollection(Arrays.asList(11, 12, 13, 14, 15, 16, 17, 18, 19));
        DataStreamSource<Integer> streamSource0 = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));


        streamSource1.map(n -> {
            Thread.sleep(50000);
            return 100 + n;
        });

        DataStream<Integer> union = streamSource0.union(streamSource2, streamSource1);

        union.print();

        //执行程序
        env.execute();
    }
}
