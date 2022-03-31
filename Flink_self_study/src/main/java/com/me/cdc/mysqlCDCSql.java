package com.me.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 * @author zs
 * @date 2021/11/8
 *
 * CREATE TABLE `aaa` (
 *   `id` int NOT NULL,
 *   `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
 *
 */
public class mysqlCDCSql {
  public static void main(String[] args) throws Exception {
    //TODO 1.基本环境准备
    //1.1 流处理环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //1.2 表执行环境
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    //创建动态表
    tableEnv.executeSql("CREATE TABLE user_info (" +
            " id INT NOT NULL," +
            " name STRING" +
            ") WITH (" +
            " 'connector' = 'mysql-cdc'," +
            " 'hostname' = 'localhost'," +
            " 'port' = '3306'," +
            " 'username' = 'root'," +
            " 'password' = '111'," +
            " 'database-name' = 'mydb'," +
            " 'table-name' = 'aaa'" +
            ")");

    //从表中查询数据
    tableEnv.executeSql("select * from user_info ").print();

    env.execute();
  }
}
