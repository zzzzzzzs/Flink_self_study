package com.me.sql;

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
 */
public class PGSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Word> stream = env.addSource(new PsqlSource());
        stream.print();
        env.execute("PostGreSQL Source to Flink demo");
    }
}

class PsqlSource extends RichSourceFunction<Word> {


    private static final long serialVersionUID = 1L;

    private Connection connection;

    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String USERNAME = "gpadmin";
        String PASSWORD = "passw0rdqwe";
        String driverClass = "org.postgresql.Driver";
        String URL = "jdbc:postgresql://47.110.73.221:2345/datacenter";
        Class.forName(driverClass);
        connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        String sql = " SELECT * FROM  public.aaa ";
        preparedStatement = connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<Word> sourceContext) throws Exception {
        try {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Word word = new Word();
                word.setA_name(resultSet.getString("a_name"));
                sourceContext.collect(word);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }
}

class Word {
    private String a_name;

    public Word() {
    }

    @Override
    public String toString() {
        return "Word{" +
                "a_name='" + a_name + '\'' +
                '}';
    }

    public Word(String a_name) {
        this.a_name = a_name;
    }

    public String getA_name() {
        return a_name;
    }

    public void setA_name(String a_name) {
        this.a_name = a_name;
    }
}