package com.me.bug;

public class Word {
    private String name;
    private Long timeStamp;

    public Word() {
    }

    public Word(String name, Long timeStamp) {
        this.name = name;
        this.timeStamp = timeStamp;
    }
    @Override
    public String toString() {
        return "Word{" +
                "name='" + name + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}