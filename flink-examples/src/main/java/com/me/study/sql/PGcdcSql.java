package com.me.study.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zs
 * @date 2021/11/5
 * TODO postgresql cdc replication mode 需要开启，现在不能用
 */
public class PGcdcSql {
    public static void main1(String[] args) throws Exception {
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


    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);



//        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);


        // 数据源表
        String sourceDDL =
                "CREATE TABLE aaa (\n" +
                        " a_name STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'postgres-cdc',\n" +
                        " 'hostname' = '47.110.73.221',\n" +
                        " 'port' = '2345',\n" +
//                        " 'slot.name' = 'flink2',\n" +
                        " 'username' = 'gpadmin',\n" +
                        " 'password' = 'passw0rdqwe',\n" +
                        " 'database-name' = 'datacenter',\n" +
                        " 'schema-name' = 'public',\n" +
                        " 'table-name' = 'aaa'\n" +
                        ")";


        // 简单的聚合处理
        String transformSQL = "SELECT * FROM aaa";
        tableEnv.executeSql(sourceDDL);
        TableResult result = tableEnv.executeSql(transformSQL);

        // 等待flink-cdc完成快照
        result.print();
        env.execute("sync-flink-cdc");
    }
}
