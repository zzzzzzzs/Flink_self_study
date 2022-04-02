package com.me.study.requirement;

import com.me.study.bean.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KeyedState_TwoSuccessiveTemperatureJumps {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);

        // map为了将数据转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Double(fields[2]), new Long(fields[1]));
        });

        // TODO 定义一个flatmap操作，检测温度跳变，输出报警
        //  每个key隔离
        //  输入 sensor_1,1547718199,35.8 如果上一次和这次温度差10度，则输出
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy(r->r.id)
                .flatMap(new TempChangeWarning(10.0));

        resultStream.print();

        env.execute();
    }

    // 实现自定义函数类
    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        // 私有属性，温度跳变阈值
        private Double threshold;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        // 定义状态，保存上一次的温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        // TODO 当两次温度差比阈值大，输出传感器id，上一次温度，本次温度
        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            // 获取状态
            Double lastTemp = lastTempState.value();

            // 如果状态不为null，那么就判断两次温度差值
            if (lastTemp != null) {
                Double diff = Math.abs(value.getTemperature() - lastTemp);
                if (diff >= threshold)
                    out.collect(new Tuple3<>(value.getId(), lastTemp, value.getTemperature()));
            }

            // 更新状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void close() throws Exception {
            super.close();
            lastTempState.clear();
        }
    }
}
