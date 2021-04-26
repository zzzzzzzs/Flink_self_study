package com.me.Transform;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class SmokeLevelSource extends RichParallelSourceFunction<SmokeLevel> {
    private Boolean running = true;
    @Override
    public void run(SourceContext<SmokeLevel> sourceContext) throws Exception {
        Random random = new Random();

        while (running) {
            if (random.nextGaussian() > 0.8) {
                sourceContext.collect(SmokeLevel.HIGH);
            } else {
                sourceContext.collect(SmokeLevel.LOW);
            }
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

