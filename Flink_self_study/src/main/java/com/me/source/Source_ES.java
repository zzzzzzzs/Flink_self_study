package com.me.source;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;

/**
 * @author zs
 * @date 2021/11/11
 */
public class Source_ES {
    public static void main(String[] args) throws Exception{
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING},
                new String[]{"from_wxid"});

        ElasticsearchInput es =  ElasticsearchInput.builder(
                Lists.newArrayList(new HttpHost("47.98.0.93", 9200)),
                "flink-to-es-pro")
                .setRowTypeInfo(rowTypeInfo)
                .build();

        // 输入
        DataStreamSource source = env.createInput(es);

        source.print();
        // 把结果输出到本地文件
//        source.writeAsText("es-student.txt", FileSystem.WriteMode.OVERWRITE);

        // 触发运行
        env.execute("esStreamApi");
    }
}
