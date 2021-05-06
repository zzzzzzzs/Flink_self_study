package com.me.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class Test1 {
    public static void main(String[] args) {
        Map<String, String> map = new HashMap() {{
            put("map1", "value1");
            put("map2", "value2");
            put("map3", "value3");
        }};
//        map.forEach((k, v) -> {
//            System.out.println("key:" + k + " value:" + v);
//        });
        map.forEach((s, s2) -> System.out.println("key:" + s + " value:" + s2));
    }
}
