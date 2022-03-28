package com.me.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zs
 * @date 2021/11/8
 *     <p>https://github.com/ypt/experiment-flink-cdc-connectors-postgres-datastream
 *     <p>TODO 用 alibaba 的 cdc（不好用）
 *     <p>https://www.cnblogs.com/xiongmozhou/p/14817641.html
 */
public class postgresCDCSql {

  public static void main(String[] args) throws Exception {
    // TODO 1.基本环境准备
    // 1.1 流处理环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 1.2 表执行环境
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // 创建动态表
    tableEnv.executeSql(
        "CREATE TABLE aaa (\n"
            + "    name STRING\n"
            + ") WITH (\n"
            + "  'connector' = 'postgres-cdc',\n"
            + "  'decoding.plugin.name' = 'pgoutput',\n"
            + "  'hostname' = 'localhost',\n"
            + "  'port' = '5432',\n"
            + "  'username' = 'postgres',\n"
            + "  'password' = '111',\n"
            + "  'database-name' = 'postgres',\n"
            + "  'schema-name' = 'public',\n"
            + "  'table-name' = 'public.aaa'\n"
            + ")");

    // 从表中查询数据
    tableEnv.executeSql("SELECT * from aaa").print();

    env.execute();
  }
}
