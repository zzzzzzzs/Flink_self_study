package com.me.state;

import com.me.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/*
TODO 在map操作中自定义算子状态变量，用的不多
* */
public class State01_DIYOperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        // sensor_1,1547718199,35.8
        env
                .socketTextStream("localhost", 9999)
                // 转换成SensorReading类型
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], new Double(fields[2]), new Long(fields[1]));
                })
                // 定义一个有状态的map操作，统计当前分区的数据个数
                .map(new MyCountMapper())
                .print();

        env.execute();
    }

    // 自定义MapFunction
    // TODO 在框架底层调用，一个任务就实例化一次
    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {
        // 定义一个本地变量，作为算子状态
        // TODO 由于框架底层一个任务就实例化该类一次，count可以++，只不过在这个任务里面可以接收不同key的数据，因此所有数据来了都会影响这个状态变量。
        //  但是如果出现故障，那么就无法处理。
        //  因此还需要实现CheckpointedFunction接口
        private Integer count = 0;
        @Override
        public Integer map(SensorReading value) throws Exception {
            // 来一个数据就将数据加1
            count++;
            return count;
        }

        // TODO 对状态做快照，Checkpoint时会调用这个方法，我们要实现具体的snapshot逻辑，比如将哪些本地状态持久化
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            System.out.println("snapshotState...");
            return Collections.singletonList(count);
        }

        // TODO 做快照，发生故障的时候再恢复
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            System.out.println("restoreState...");
            for( Integer num: state )
                count += num;
        }
    }
}
