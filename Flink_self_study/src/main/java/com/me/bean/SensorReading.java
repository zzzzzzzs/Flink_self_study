package com.me.bean;

import java.sql.Timestamp;

// 温度传感器读数POJO Class
public class SensorReading {
    public String id;
    public Double temperature;
    public Long timestamp;

    public SensorReading() {
    }

    public SensorReading(String id, Double temperature, Long timestamp) {
        this.id = id;
        this.temperature = temperature;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public Double getTemperature() {
        return temperature;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public static SensorReading of(String id, Double temperature, Long timestamp) {
        return new SensorReading(id, temperature, timestamp);
    }
    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", temperature=" + temperature +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
