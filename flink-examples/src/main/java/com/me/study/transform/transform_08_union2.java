package com.me.study.transform;


import com.me.study.bean.SensorReading;
import com.me.study.source.SensorSource;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/* TODO union合并多条类型一样的流，输出也是类型一样的
    多流转换算子
    多条流的输入类型是一样的，合并成一个单一输出流，输出类型也是一样的
    事件合流的方式为 FIFO 方式。操作符并不会产生一个特定顺序的事件流。union 操作符也不会进行去重。每一个输入事件都被发送到了下一个操作符。
    谁先进来谁就先出去，并不能保证哪条流先进来。如果有数据流中没有数据，则union以后会去掉没有数据的流
    例如kafka，队列，衍生开来 背压问题如何产生的？
        进来的太快，出去的太慢就是背压。
        进来的太慢，出去的太快出现饥饿。
    如何解决背压问题？
        1.将队列加大
        2.在队列前面再加一个队列，削峰，减缓数据进入队列的速度
        3.把程序优化，加快消费的速度
 */
public class transform_08_union2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> intStream = env.fromElements("a", "b", "c");
        DataStreamSource<String> stringStream = env.fromElements("e", "f", "g");
        // 把两个流的数据合成一条流发送下去
        DataStream<String> union = intStream.union(stringStream);
        union.print(">>>");
        env.execute();
    }
}
