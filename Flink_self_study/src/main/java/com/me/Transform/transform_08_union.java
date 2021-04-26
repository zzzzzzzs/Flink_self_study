package com.me.Transform;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/* TODO union合并多条类型一样的流，输出也是类型一样的
    多流转换算子
    多条流的输入类型是一样的，合并成一个单一输出流，输出类型也是一样的
    事件合流的方式为 FIFO 方式。操作符并不会产生一个特定顺序的事件流。union 操作符也不会进行去重。每一个输入事件都被发送到了下一个操作符。
    谁先进来谁就先出去，并不能保证哪条流先进来。
    例如kafka，队列，衍生开来 背压问题如何产生的？
        进来的太快，出去的太慢就是背压。
        进来的太慢，出去的太快出现饥饿。
    如何解决背压问题？
        1.将队列加大
        2.在队列前面再加一个队列，削峰，减缓数据进入队列的速度
        3.把程序优化，加快消费的速度
 */
public class transform_08_union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> sensor_1 = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));
        SingleOutputStreamOperator<SensorReading> sensor_2 = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_2"));
        SingleOutputStreamOperator<SensorReading> sensor_3 = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_3"));

        DataStream<SensorReading> union = sensor_1.union(sensor_2, sensor_3);

        union.print();

        env.execute();
    }
}
