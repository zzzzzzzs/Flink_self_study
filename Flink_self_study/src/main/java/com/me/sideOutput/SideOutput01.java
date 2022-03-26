package com.me.sideOutput;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutput01 {
  /* TODO 定义侧输出标签，注意有花括号
      OutputTag这个类是带泛型的，但是构造方法没有泛型，用的反射获取到类型信息的时候就会有泛型擦除就得不到类型信息了，所以带上{}，用匿名内部类的方式实现
   */
  private static OutputTag<String> late = new OutputTag<String>("late-readings") {};
  private static OutputTag<String> notLate = new OutputTag<String>("not-late") {};

  public static void main(String[] args) throws Exception {
    // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Configuration conf = new Configuration();
    conf.setString(RestOptions.BIND_PORT, "8081-8089");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
    env.setParallelism(1);

    SingleOutputStreamOperator<String> result = env.addSource(new CustomSource())
      .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
          @Override
          public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
            return element.f1;
          }
        }))
      // TODO 未迟到的元素正常输出，迟到的元素放到侧输出流上
      .process(new ProcessFunction<Tuple2<String, Long>, String>() {
        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
          System.out.println("当前水位线：" + ctx.timerService().currentWatermark());
          if (value.f1 < ctx.timerService().currentWatermark()) {
            ctx.output(late, "迟到元素来了，时间戳是：" + value.f1);
          } else {
            ctx.output(notLate, "没有迟到的元素到了，时间戳是：" + value.f1);
          }
        }
      });

    result.getSideOutput(notLate).print();

    result.getSideOutput(late).print();

    env.execute();
  }

  public static class CustomSource implements SourceFunction<Tuple2<String, Long>> {
    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
      ctx.collect(Tuple2.of("a", 1000L));
      Thread.sleep(1000L);
      ctx.collect(Tuple2.of("a", 10 * 1000L));
      Thread.sleep(1000L);
      ctx.collect(Tuple2.of("a", 5000L));
    }

    @Override
    public void cancel() {

    }
  }
}
