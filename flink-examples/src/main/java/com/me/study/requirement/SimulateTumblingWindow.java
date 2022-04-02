package com.me.study.requirement;

import com.me.study.bean.SensorReading;
import com.me.study.source.SensorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// TODO 使用 KeyedProcessFunction+状态+定时器 模拟滚动窗口
public class SimulateTumblingWindow {
  public static void main(String[] args) throws Exception {
    // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Configuration conf = new Configuration();
    conf.setString(RestOptions.BIND_PORT, "8081-8089");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
    env.setParallelism(1);

    env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"))
      .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
          @Override
          public long extractTimestamp(SensorReading element, long recordTimestamp) {
            return element.timestamp;
          }
        }))
      .keyBy(r -> r.id).process(new KeyedProcessFunction<String, SensorReading, String>() {
        private MapState<Long, Double> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
          super.open(parameters);
          mapState =
            getRuntimeContext().getMapState(new MapStateDescriptor<Long, Double>("map", Types.LONG, Types.DOUBLE));
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<String> collector)
          throws Exception {
          long windowStart = sensorReading.timestamp - sensorReading.timestamp % 5000L;
          long windowEnd = windowStart + 5000L - 1L;
          if (!mapState.contains(windowStart)) {
            mapState.put(windowStart, sensorReading.temperature);
          } else {
            mapState.put(windowStart, mapState.get(windowStart) + sensorReading.temperature);
          }
          context.timerService().registerEventTimeTimer(windowEnd);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
          super.onTimer(timestamp, ctx, out);
          out.collect(new Timestamp(timestamp - 4999L).toString() + "~" + new Timestamp(timestamp + 1L).toString()
            + "窗口的温度总和是：" + mapState.get(timestamp - 4999L));
        }
      }).print();

    env.execute();
  }
}
