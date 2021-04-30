package com.me.WaterMark;

public class Word {
    public String word;
    public Long timestamp; // 毫秒时间戳
    public Word() {
    }

    public Word(String word, Long timestamp) {
        this.word = word;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Word{" +
                "word='" + word + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
