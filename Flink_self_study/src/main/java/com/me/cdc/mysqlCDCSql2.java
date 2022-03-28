package com.me.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zs
 * @date 2021/11/8
 */
public class mysqlCDCSql2 {
  public static void main(String[] args) throws Exception {
    // TODO 1.基本环境准备
    // 1.1 流处理环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 1.2 表执行环境
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // 创建动态表
    tableEnv.executeSql(
        "CREATE TABLE aaa\n"
            + "(\n"
            + "    id   INT,\n"
            + "    name STRING,\n"
            + "    primary key (`id`) NOT ENFORCED\n"
            + ")\n"
            + "    WITH ( 'connector' = 'mysql-cdc',\n"
            + "        'hostname' = 'localhost',\n"
            + "        'port' = '3306',\n"
            + "        'username' = 'root',\n"
            + "        'password' = '111',\n"
            + "        'database-name' = 'mydb',\n"
            + "        'table-name' = 'information_schema.`COLUMNS`',\n"
            + "        'scan.incremental.snapshot.enabled' = 'false'\n"
            + "        )");

    // 从表中查询数据
    tableEnv.executeSql("select * from aaa").print();

    env.execute();
  }
}
