package com.me.cdc;

import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author zs
 * @date 2021/11/8
 *     <p>https://github.com/ypt/experiment-flink-cdc-connectors-postgres-datastream
 *     <p>TODO 用 alibaba 的 cdc
 *     <p>https://www.cnblogs.com/xiongmozhou/p/14817641.html
 */
public class postgresCDC {
  public static void main(String[] args) throws Exception {
    // Typically, env can be set up this way if you don't care to bring up the web UI locally. The
    // getExecutionEnvironment() function will return the appropriate env depending on context of
    // execution
    // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // However for easy local experimentation, we can explicitly specify a local streaming execution
    // environment,
    // and also bring up a Web UI and REST endpoint - available at: http://localhost:8081
    //
    // Do NOT do this when actually packaging for deployment. Instead, just use
    // getExecutionEnvironment()
    // StreamExecutionEnvironment env =
    // StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // For more builder options, see:
    // https://github.com/ververica/flink-cdc-connectors/blob/master/flink-connector-postgres-cdc/src/main/java/com/alibaba/ververica/cdc/connectors/postgres/PostgreSQLSource.java#L43
    SourceFunction<String> sourceFunction1 =
        PostgreSQLSource.<String>builder()
            // The pgoutput logical decoding plugin is only supported in Postgres 10+
            // https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-pgoutput
            .decodingPluginName("pgoutput")
            // Slot names should be unique per Postgres node
            // .slotName("flink1")
            .hostname("localhost")
            .port(5432)
            .database("postgres")
            .schemaList("public")
            .tableList("aaa")
            .username("postgres")
            .password("111")

            // This simple deserializer just toString's a SourceRecord. We'll want to impl our own
            // to extract the
            // data that we want.
            // See:
            // https://github.com/ververica/flink-cdc-connectors/blob/release-1.4/flink-connector-debezium/src/main/java/com/alibaba/ververica/cdc/debezium/StringDebeziumDeserializationSchema.java
            .deserializer(new StringDebeziumDeserializationSchema())
            .build();

    DataStream<String> stream1 = env.addSource(sourceFunction1);

    stream1.print().setParallelism(1);

    env.execute("experiment");
  }
}
