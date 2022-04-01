package com.me.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author zs
 * @date 2021/11/8
 *
 * TODO cdc2.0 以上的表必须有主键
 *
 */
public class mysqlCDC {
  public static void main(String[] args) throws Exception {
    MySqlSource<String> mySqlSource =
        MySqlSource.<String>builder()
            .hostname("localhost")
            .port(3336)
            .databaseList("mydb") // set captured database
            .tableList("mydb.aaa") // set captured table
            .username("root")
            .password("111")
            .startupOptions(StartupOptions.initial())
            .deserializer(
                new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // enable checkpoint
    //    env.enableCheckpointing(3000);

    env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
        // set 4 parallel source tasks
        .setParallelism(1)
        .print(">>>")
        .setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute("Print MySQL Snapshot + Binlog");
  }
}
