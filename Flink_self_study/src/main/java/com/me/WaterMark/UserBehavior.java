package com.me.WaterMark;

import java.sql.Timestamp;

// 封装UserBehavior的 POJO类
public class UserBehavior {
    public String userId;
    public String itemId;
    public String categoryId;
    public String behaviorType;
    public Long timestamp;


    public UserBehavior(String userId, String itemId, String categoryId, String behaviorType, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behaviorType = behaviorType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", behaviorType='" + behaviorType + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}