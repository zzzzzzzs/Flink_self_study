package com.me.state;

import com.me.bean.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/*
TODO 每传入一条数据，有状态的算子任务都会读取和更新状态。由于有效的状态访问对于处理数据的低延迟至关重要，
        因此每个并行任务(子任务)都会在本地维护其状态，以确保快速的状态访问。
	状态的存储、访问以及维护，由一个可插入的组件决定，这个组件就叫做状态后端（state backend）
	状态后端主要负责两件事：
        本地的状态管理
        将检查点（checkpoint）状态写入远程存储

* */

public class State03_StateBackend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoints/fs"));
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/checkpoints/rocksdb"));

        // 2. 检查点配置
        env.enableCheckpointing(300);

        // 高级选项
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        // 3. 重启策略配置
        // 固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        // 失败率重启
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Double(fields[2]), new Long(fields[1]));
        });

        dataStream.print();
        env.execute();
    }
}
