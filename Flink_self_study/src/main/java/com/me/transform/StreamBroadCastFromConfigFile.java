package com.me.transform;

import cn.hutool.core.io.file.FileReader;
import com.me.bean.SmokeLevel;
import com.me.source.SmokeLevelSource;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

/**
 * @author zs
 * @date 2021/11/19
 * TODO 注意使用 StreamExecutionEnvironment，流处理。读取数据库中的配置文件，对流数据进行处理。及流表与维表进行关联处理。
 *  这里使用的是文件而不是数据库
 *
 * https://www.codeleading.com/article/2749932764/
 * 也可以看我写的动态分流程序
 */
public class StreamBroadCastFromConfigFile {
    private static final Logger LOG = LoggerFactory.getLogger(StreamBroadCastFromConfigFile.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文件中的信息，可以换成数据库。这里这样读取文件完全是为了方便后期换成数据库
        DataStreamSource<String> configDS = env.addSource(new RichSourceFunction<String>() {
            List<String> config;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                LOG.info("打开广播流");
                FileReader fileReader = new FileReader("textfile");
                config = fileReader.readLines();
            }

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (String ele : config) {
                    LOG.info("广播数据:{}", ele);
                    ctx.collect(ele);
                }
            }

            @Override
            public void cancel() {
            }

            @Override
            public void close() throws Exception {
                LOG.info("关闭广播流");
            }
        });
        //1、定义数据广播的规则：
        MapStateDescriptor<String, String> confStateDescriptor = new MapStateDescriptor<>("config-set", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        //2、将configDS变成广播流
        BroadcastStream<String> confBS = configDS.broadcast(confStateDescriptor);

        //主流
        DataStreamSource<SmokeLevel> mainDS = env.addSource(new SmokeLevelSource());

        //3、dataStream对广播的数据进行关联（使用connect进行连接）
        DataStream<String> result = mainDS.connect(confBS).process(new BroadcastProcessFunction<SmokeLevel, String, String>() {
            MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("config-set"
                    , BasicTypeInfo.STRING_TYPE_INFO
                    , BasicTypeInfo.STRING_TYPE_INFO);


            List<SmokeLevel> list = new ArrayList<>(); // 用来缓存未处理的数据
            Set<String> configSet = new HashSet<>(); // 保存广播下来的数据

            @Override
            public void open(Configuration parameters) throws Exception {
                LOG.info("主流开始。。。");
            }

            // 主流
            @Override
            public void processElement(SmokeLevel value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取广播状态
                ReadOnlyBroadcastState<String, String> tableProcessState = ctx.getBroadcastState(mapStateDescriptor);
                // TODO 正常应该这样获取广播变量中的数据，但是现在不能获取长度
//                tableProcessState.contains();
                LOG.info("获取主流中的数据" + value);
//                LOG.info("广播变量中的内容" + ;);
                if (configSet.isEmpty()) {
                    list.add(value);
                    LOG.info("此时广播数据为空，要将主流数据: {} 缓存起来", value);
                }else { // 如果广播数据不为空，则处理数据
                    LOG.info("处理主流数据:{}", value);
                    if (!list.isEmpty()) {
                        list.forEach(smokeLevel -> LOG.info("处理缓存数据:{}", smokeLevel));
                    }
                    list.clear();
                }
                LOG.info("-------------------------");
            }

            // 广播流
            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                LOG.info("接收广播流中的数据:{}",value);
                //获取广播状态
                BroadcastState<String, String> tableProcessState = ctx.getBroadcastState(mapStateDescriptor);
                // 放到广播变量中
                tableProcessState.put(value, value);
                configSet.add(value);
                LOG.info("*************************************");
            }
        });

        result.print();
        env.execute("broadcast test");
    }
}
