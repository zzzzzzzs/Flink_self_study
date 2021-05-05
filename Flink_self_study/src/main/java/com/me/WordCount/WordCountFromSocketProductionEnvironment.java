package com.me.WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class WordCountFromSocketProductionEnvironment {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.disableOperatorChaining();

        // 用parameter tool工具从程序启动参数中提取配置项，生产环境
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 从socket文本流读取数据
        // 在Run->Edit Configurations->Program arguments中设置：--host localhost --port 7777
        DataStream<String> inputDataStream = env.socketTextStream(host, port);


        // 基于数据流进行转换计算
        inputDataStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] ss = value.split(" ");
                        for (String s: ss) {
                            out.collect(Tuple2.of(s, 1));
                        }
                    }
                })
                .keyBy(r->r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1+value2.f1);
                    }
                })
                .print()
                .setParallelism(1)
                ;

        // 执行任务
        env.execute();
    }
}
