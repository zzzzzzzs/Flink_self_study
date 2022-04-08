package com.me.study.state;

import com.me.study.bean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
TODO 键控状态：KeyedState 经常使用
* */
public class State04_KeyedState_Checkpoint {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    // 1. 状态后端配置 StateBackend
    env.setStateBackend(new HashMapStateBackend());
    env.enableCheckpointing(1000);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointTimeout(60000L);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
    env.getCheckpointConfig()
        .setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
    env.getCheckpointConfig().enableUnalignedCheckpoints();
    env.getCheckpointConfig().setCheckpointStorage("hdfs://node1:8020/flink-study/State04_KeyedState_Checkpoint/ck");
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
    env.setRestartStrategy(
        RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));

    // socket文本流
    env.socketTextStream("localhost", 7777)
        // 转换成SensorReading类型
        // sensor_1,35.8,1547718199
        .map(
            line -> {
              String[] fields = line.split(",");
              return new SensorReading(
                  fields[0], Double.valueOf(fields[1]), Long.valueOf(fields[2]));
            })
        // 定义一个有状态的map操作，统计当前sensor数据个数
        .keyBy(r -> r.id)
        // TODO 定义一个有状态的map操作，统计当前sensor数据的个数
        .map(new MyKeyCountMapper())
        .print();

    env.execute();
  }

  /***
   * TODO 键控状态是根据输入数据流中定义的键（key）来维护和访问的。因此在使用前需要有keyBy。
   *  只有使用运行时上下文，才能区分是哪个key，要不然和算子状态一样了（所有的数据都会影响此状态变量）。
   *  只有在RichFunction才能获取运行时上下文。
   *  Flink为每个键值维护一个状态实例，并将具有相同键的所有数据，都分区到同一个算子任务中，这个任务会维护和处理这个key对应的状态。
   *  当任务处理一条数据时，它会自动将状态的访问范围限定为当前数据的key。因此，具有相同key的所有数据都会访问相同的状态。
   */
  // 自定义RichMapFunction
  public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {
    private ValueState<Integer> keyCountState;

    // 其它类型状态的声明
    private ListState<String> myListState;
    private MapState<String, Double> myMapState;
    private ReducingState<SensorReading> myReducingState;

    @Override
    public void open(Configuration parameters) throws Exception {
      // TODO getRuntimeContext 需要在生命周期中使用，名称不能相同，根据名称实例化，且只能实例化一次
      // TODO 这种方法已经被弃用了，最好还是在map中判断一下null，然后给初值
      keyCountState =
          getRuntimeContext()
              .getState(new ValueStateDescriptor<Integer>("key-count", Integer.class, 0));
      //            keyCountState = getRuntimeContext().getState(new
      // ValueStateDescriptor<Integer>("key-count", Types.INT));
      myListState =
          getRuntimeContext()
              .getListState(new ListStateDescriptor<String>("my-list", String.class));
      myMapState =
          getRuntimeContext()
              .getMapState(
                  new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
    }

    @Override
    public Integer map(SensorReading value) throws Exception {
      // 其它状态API调用
      // list state
      for (String str : myListState.get()) {
        System.out.println(str);
      }
      myListState.add("hello");
      // map state
      myMapState.get("1");
      myMapState.put("2", 12.3);
      myMapState.remove("2");
      // reducing state
      //            myReducingState.add(value);

      myMapState.clear();

      Integer count = keyCountState.value();
      count++;
      keyCountState.update(count);
      return count;
    }
  }
}
