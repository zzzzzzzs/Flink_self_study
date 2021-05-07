package com.me.test;

import java.util.HashMap;
import java.util.Map;

public class Test3 {
    public static void main(final String[] args) {
        Map<String, String> map = new HashMap() {{
            put("map1", "value1");
            put("map2", "value2");
            put("map3", "value3");
        }};
    }
}

