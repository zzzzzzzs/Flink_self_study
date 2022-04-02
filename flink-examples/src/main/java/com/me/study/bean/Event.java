package com.me.study.bean;

public class Event {
    public String userId;
    public String eventType;
    public Long timestamp;

    public Event(String userId, String eventType, Long timestamp) {
        this.userId = userId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    public Event() {
    }

    @Override
    public String toString() {
        return "Event{" +
                "userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
