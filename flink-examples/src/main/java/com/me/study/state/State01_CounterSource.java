package com.me.study.state;

import com.me.study.bean.SensorReading;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/*
TODO 在 source 端使用自定义 OperatorState
  在典型的有状态 Flink 应用中你无需使用算子状态。它大都作为一种特殊类型的状态使用。
  用于实现 source/sink，以及无法对 state 进行分区而没有主键的这类场景中。
* */
public class State01_CounterSource {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    env.addSource(new CounterSource()).print();

    env.execute();
  }
}

class CounterSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {

  /** current offset for exactly once semantics */
  private Long offset = 0L;

  /** flag for job cancellation */
  private volatile boolean isRunning = true;

  /** 存储 state 的变量. */
  private ListState<Long> state;

  @Override
  public void run(SourceContext<Long> ctx) {
    final Object lock = ctx.getCheckpointLock();

    while (isRunning) {
      // output and state update are atomic
      synchronized (lock) {
        ctx.collect(offset);
        offset += 1;
      }
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    state =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("state", LongSerializer.INSTANCE));

    // 从我们已保存的状态中恢复 offset 到内存中，在进行任务恢复的时候也会调用此初始化状态的方法
    for (Long l : state.get()) {
      offset = l;
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    state.clear();
    state.add(offset);
  }
}
