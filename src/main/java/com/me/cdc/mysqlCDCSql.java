package com.me.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 * TODO 有问题，在 initial 模式下，抓不到变化数据
 * */
public class mysqlCDCSql {
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
<<<<<<< HEAD:src/main/java/com/me/cdc/mysqlCDCSql.java
            + "        'table-name' = 'aaa',\n"
            + "        'scan.startup.mode' = 'initial',\n"
<<<<<<< HEAD:src/main/java/com/me/cdc/mysqlCDCSql.java
=======
=======
            + "        'table-name' = 'mydb.aaa',\n"
>>>>>>> parent of 82ee9a5 (更新pom结构):Flink_self_study/src/main/java/com/me/cdc/mysqlCDCSql.java
>>>>>>> f9fcac5db06528ab1f3d17d55c6174882c451317:Flink_self_study/src/main/java/com/me/cdc/mysqlCDCSql.java
            + "        'scan.incremental.snapshot.enabled' = 'true'\n"
            + "        )");

    // 从表中查询数据
    tableEnv.executeSql("select * from aaa").print();

    env.execute();
  }
}
