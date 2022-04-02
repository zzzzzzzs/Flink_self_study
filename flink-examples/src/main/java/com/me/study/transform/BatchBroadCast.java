package com.me.study.transform;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zs
 * @date 2021/11/19
 * TODO 注意使用 ExecutionEnvironment，批处理
 * https://blog.csdn.net/digua930126/article/details/104014199
 */
public class BatchBroadCast {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> textFile = environment.readTextFile("D:\\workspace\\zs\\code\\Flink_self_study\\Flink_self_study\\src\\main\\resources\\textfile");

        List<String> list = new ArrayList<>();

        list.add("zhangsan");
        list.add("lisi");

        DataSource<String> whiteDs = environment.fromCollection(list);

        FilterOperator<String> f1 = textFile.filter(new RichFilterFunction<String>() {
            List<String> whiteNames = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                whiteNames = getRuntimeContext().getBroadcastVariable("white-name");
            }

            @Override
            public boolean filter(String value) throws Exception {
                for (String whileName : whiteNames) {
                    if (value.contains(whileName)) {
                        return true;
                    }
                }
                return false;
            }
        });

        //f1 operator算子可以得到广播变量。whiteDs的数据广播出去
        FilterOperator<String> f2 = f1.withBroadcastSet(whiteDs, "white-name");

        f2.print();

        System.out.println("=======================================");

        FilterOperator<String> f3 = f2.filter(new RichFilterFunction<String>() {

            List<String> whiteNames = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                whiteNames = getRuntimeContext().getBroadcastVariable("white-name");
            }

            @Override
            public boolean filter(String value) throws Exception {
                for (String whileName : whiteNames) {
                    if (value.contains(whileName)) {
                        return true;
                    }
                }
                return false;
            }
        });
        //就理解为谁用广播变量就给谁赋值
        FilterOperator<String> f4 = f3.withBroadcastSet(whiteDs, "white-name");
        f4.print();
    }
}
