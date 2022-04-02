package com.me.study.transform;

import cn.hutool.core.io.file.FileReader;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.dictionary.DictionaryFactory;
import org.apdplat.word.segmentation.Word;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zs
 * @date 2021/11/19
 * TODO 注意使用 StreamExecutionEnvironment，流处理。读取数据库中的配置文件，对流数据进行处理。及流表与维表进行关联处理。
 *  这里使用的是文件而不是数据库
 *  有可能主流中的数据先于广播流数据到，因此先将未处理的主流中的数据缓存起来。
 *  使用配置文件中的数据进行分词
 * https://www.codeleading.com/article/2749932764/
 * 也可以看我写的动态分流程序
 */
public class StreamBroadCastFromConfigFilePro2 {
    private static final Logger LOG = LoggerFactory.getLogger(StreamBroadCastFromConfigFilePro2.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取文件中的信息，可以换成数据库。这里这样读取文件完全是为了方便后期换成数据库
        DataStreamSource<String> configDS = env.addSource(new RichSourceFunction<String>() {
            List<String> config;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                DictionaryFactory.getDictionary().clear();
                LOG.info("打开广播流");
                FileReader fileReader = new FileReader("textfile");
                config = fileReader.readLines();
                DictionaryFactory.getDictionary().addAll(config);
                LOG.info("共加载了 {} 条配置数据", config.size());
                LOG.info("提前加载分词功能，不会影响下游程序暂停");
                WordSegmenter.seg("test");
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
        DataStreamSource<String> mainDS = env.addSource(new RichSourceFunction<String>() {
            private Boolean running = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (running) {
                    ctx.collect("这个价格怎么说");
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        //3、dataStream对广播的数据进行关联（使用connect进行连接）
        DataStream<String> result = mainDS.connect(confBS).process(new BroadcastProcessFunction<String, String, String>() {
            MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("config-set"
                    , BasicTypeInfo.STRING_TYPE_INFO
                    , BasicTypeInfo.STRING_TYPE_INFO);

            List<String> list = new ArrayList<>(); // 用来缓存未处理的数据
            boolean isExistBroadcastState = false; // 判断是否有广播变量
            int configSize = 0; // 加载配置表中的个数

            @Override
            public void open(Configuration parameters) throws Exception {
                LOG.info("主流开始。。。");
            }

            // 主流
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取广播状态
                ReadOnlyBroadcastState<String, String> tableProcessState = ctx.getBroadcastState(mapStateDescriptor);
                LOG.info("获取主流中的数据:" + value);
                if (false == isExistBroadcastState) {
                    list.add(value);
                    LOG.info("此时广播数据为空，要将主流数据: {} 缓存起来", value);
                    LOG.info("此时缓存中有 {} 条数据待处理", list.size());
                } else { // 如果广播数据不为空，则处理数据
                    LOG.info("处理主流数据:{}", value);
                    LOG.info("此时缓存中有 {} 条数据待处理", list.size());
                    List<Word> seg = WordSegmenter.seg(value);
                    for (Word ele : seg) {
                        boolean contains = tableProcessState.contains(ele.getText());
                        if (true == contains) {
                            LOG.info("命中了:" + ele.getText());
                        }
                    }
                    LOG.info("此时缓存中有 {} 条数据待处理", list.size());
                    if (!list.isEmpty()) {
                        list.forEach(ele -> {
                            LOG.info("处理缓存数据:{}", ele);
                            List<Word> words = WordSegmenter.seg(value);
                            for (Word word : words) {
                                boolean contains = false;
                                try {
                                    contains = tableProcessState.contains(word.getText());
                                } catch (Exception e) {
                                    LOG.error("出现异常:{}", e);
                                }
                                if (true == contains) {
                                    LOG.info("缓存数据命中了:{}", word.getText());
                                }
                            }
                        });
                        list.clear();
                    }
                }
                LOG.info("-------------------------");
            }

            // 广播流
            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                isExistBroadcastState = true;
                LOG.info("接收广播流中的数据:{}", value);
                boolean contains = DictionaryFactory.getDictionary().contains(value);
                if (contains == true) {
                    LOG.info("分词前缀树加载词:{}", value);
                    LOG.info("分词前缀树共加载词: {} 个", ++configSize);
                }
                //获取广播状态
                BroadcastState<String, String> tableProcessState = ctx.getBroadcastState(mapStateDescriptor);
                // 放到广播变量中
                tableProcessState.put(value, value);
                LOG.info("*************************************");
            }
        });

        result.print();
        env.execute("broadcast test");
    }
}
