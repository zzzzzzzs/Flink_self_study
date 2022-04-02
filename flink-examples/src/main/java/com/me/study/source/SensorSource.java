package com.me.study.source;

import com.me.study.bean.SensorReading;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

// 自定义数据源
// 泛型是流中元素的类型
public class SensorSource extends RichParallelSourceFunction<SensorReading> {
    private Boolean running = true;
    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        // run用来产生数据
        Random random = new Random();

        String[] sensorIds = new String[10];
        double[] curFTemps = new double[10];
        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + (i + 1);
            curFTemps[i] = 65 + random.nextGaussian() * 20;
        }

        while (running) {
            long curTs = Calendar.getInstance().getTimeInMillis(); // 毫秒时间戳
            for (int i = 0; i < 10; i++) {
                curFTemps[i] += random.nextGaussian() * 0.5;
                // 使用了上下文的collect方法来向下游发出数据
                sourceContext.collect(SensorReading.of(sensorIds[i], curFTemps[i], curTs));
            }
            Thread.sleep(1000L); // 每隔100ms发送一次传感器数据
        }
    }

    @Override
    public void cancel() {
        // 在取消任务的时候调用cancel函数，比如在UI界面或者命令行
        // 底层调用的
        running = false;
    }
}
