package com.me.study.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zs
 * @date 2021/11/5
 * 使用 flinksql 从文件中创建2张表
 */
public class FileSql {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 表执行环境
        StreamTableEnvironment userTableEnv = StreamTableEnvironment.create(env);

        //创建动态表
        userTableEnv.executeSql("CREATE TABLE user_info (" +
                " user_id INT," +
                " user_name STRING," +
                " age INT" +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'D:\\workspace\\zs\\code\\Flink_self_study\\Flink_self_study\\src\\main\\resources\\user_info.csv',"+
                " 'format' = 'csv'" +
                ")");


        userTableEnv.executeSql("CREATE TABLE work_info (" +
                " user_id INT," +
                " work STRING" +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'D:\\workspace\\zs\\code\\Flink_self_study\\Flink_self_study\\src\\main\\resources\\work_info.csv',"+
                " 'format' = 'csv'" +
                ")");

        /**
            是 撤回，+是操作后，I是插入，U是更新，D是删除
            如 -U是撤回前的数据，+U是更新后的数据
         **/
        // 2张表做joins
        TableResult execute = userTableEnv.sqlQuery("select t1.user_id,\n" +
                "       t1.user_name,\n" +
                "       t2.work\n" +
                "from user_info t1\n" +
                "         left join (\n" +
                "    select *\n" +
                "    from work_info\n" +
                ") t2 on t1.user_id = t2.user_id").execute();
//        userTableEnv.executeSql("select * from work_info ").print(); // 这种会打印中间过程


        // 这样操作只会打印最后的结果，不会打印中间过程
        execute.print();

        //TODO print()已经触发了，不需要这里了
//        env.execute();
    }
}
