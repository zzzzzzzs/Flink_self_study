package com.me.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zs
 * @date 2021/11/5
 * 使用 flinksql 从文件中创建2张表
 */
public class PGcdcSql {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建动态表
        tableEnv.executeSql("CREATE TABLE aaa (" +
                " a_name STRING" +
                ") WITH (" +
                " 'connector' = 'postgres-cdc'," +
                " 'hostname' = '47.110.73.221'," +
                " 'port' = '2345'," +
                " 'username' = 'gpadmin'," +
                " 'password' = 'passw0rdqwe'," +
                " 'database-name' = 'datacenter'," +
                " 'schema-name' = 'public'," +
                " 'table-name' = 'aaa'" +
                ")");

        //从表中查询数据
        TableResult execute = tableEnv.sqlQuery("select * from aaa ").execute();
        execute.print();
//        env.execute();
    }
}
