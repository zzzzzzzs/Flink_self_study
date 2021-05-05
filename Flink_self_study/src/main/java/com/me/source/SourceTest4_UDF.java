package com.me.source;

import com.me.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;


public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 自定义source 一般自己实时模拟数据
        DataStream<SensorReading> dataStream = env.addSource( new MySensorSource() );

        // 打印输出
        dataStream.print();

        env.execute();
    }

    // 实现自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading>{
        // 定义一个标识位，用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();

            // 设置10个传感器的初始温度
            HashMap<String, Double> sensorTempMap = new HashMap<>();
            for( int i = 0; i < 10; i++ ){
                sensorTempMap.put("sensor_" + (i+1), 60 + random.nextGaussian() * 20);
            }

            while (running){
                for( String sensorId: sensorTempMap.keySet() ){
                    // 在当前温度基础上随机波动
                    Double newtemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newtemp);
                    ctx.collect(new SensorReading(sensorId, newtemp, System.currentTimeMillis()));
                }
                // 控制输出频率
                Thread.sleep(1000L);
            }
        }
        // 在取消任务的时候调用cancel函数，比如在UI界面或者命令行
        // 底层调用的
        @Override
        public void cancel() {
            running = false;
        }
    }}
