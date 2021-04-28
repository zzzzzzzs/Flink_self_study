package com.me.WaterMark;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ccc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        stream
                .map(
                        new MapFunction<String, Word>() {
                            @Override
                            public Word map(String value) throws Exception {
                                System.out.println("22222222222222222222222");
                                String[] s = value.split(" ");
                                return new Word(s[0], Long.valueOf(s[1]) * 1000);
                            }
                        }
                )
                .keyBy(
                        new KeySelector<Word, String>() {
                            @Override
                            public String getKey(Word value) throws Exception {
                                System.out.println("--------" + value.name);
                                return value.name;
                            }
                        }
                )
                .print();

        env.execute();
    }

    private static class Word {
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
}
