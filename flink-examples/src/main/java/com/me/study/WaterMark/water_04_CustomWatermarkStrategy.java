package com.me.study.WaterMark;

import com.me.study.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/*
    TODO 自定义水位线的产生
        一般不使用自定义水位线，因为自带的就可以解决工作中的所有问题
* */
public class water_04_CustomWatermarkStrategy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("./file/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0],arr[1],arr[2],arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behaviorType.equals("pv"))
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();

        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<UserBehavior> {
        @Override
        public TimestampAssigner<UserBehavior> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<UserBehavior>() {
                @Override
                public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                    return element.timestamp; // 告诉程序数据源里的时间戳是哪一个字段
                }
            };
        }

        @Override
        public WatermarkGenerator<UserBehavior> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomBoundedOutOfOrdernessAssigner();
        }
    }

    public static class CustomBoundedOutOfOrdernessAssigner implements WatermarkGenerator<UserBehavior> {
        private Long delayTime = 5000L; // 最大延迟时间
        private Long maxTs = -Long.MAX_VALUE + delayTime + 1L; // 观察到的最大时间戳
        @Override
        public void onEvent(UserBehavior event, long eventTimestamp, WatermarkOutput output) {
            // 每来一条数据就调用一次
            maxTs = Math.max(event.timestamp, maxTs); // 更新最大时间戳
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发射水位线
            // 默认200ms调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }

}
