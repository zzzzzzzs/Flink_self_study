package com.me.study.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author zs
 * @date 2021/11/5
 * TODO postgresql cdc replication mode 需要开启，现在不能用
 */
public class Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.addSource(new RichSourceFunction<String>() {
            private static final long serialVersionUID = 1L;
            private Connection msCN;
            private PreparedStatement msSM;

            @Override
            public void open(Configuration parameters) throws Exception {
                String msUN = "dev_zhaoshuo";
                String msPW = "j%nZa#KnmX:B!^hQ!";
                String msDC = "com.mysql.jdbc.Driver";
                String msURL = "jdbc:mysql://47.114.124.5:3306/dfsw_basebiz";
                Class.forName(msDC);
                msCN = DriverManager.getConnection(msURL, msUN, msPW);
                String msSql = "SELECT wxid FROM dfsw_wechat_info";
                msSM = msCN.prepareStatement(msSql);
            }

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                ResultSet resultSet = msSM.executeQuery();
                while (resultSet.next()) {
                    ctx.collect(resultSet.getString("wxid"));
                }
            }

            @Override
            public void cancel() {
                System.out.println("运行结束");
            }
        });
        stream.print();
        env.execute("PostGreSQL Source to Flink demo");
    }
}