package com.ververica.field.dynamicrules.sources;

import static com.ververica.field.config.Parameters.GCP_PROJECT_NAME;
import static com.ververica.field.config.Parameters.GCP_PUBSUB_RULES_SUBSCRIPTION;
import static com.ververica.field.config.Parameters.RULES_SOURCE;
import static com.ververica.field.config.Parameters.RULES_TOPIC;
import static com.ververica.field.config.Parameters.SOCKET_PORT;

import com.ververica.field.config.Config;
import com.ververica.field.dynamicrules.KafkaUtils;
import com.ververica.field.dynamicrules.Rule;
import com.ververica.field.dynamicrules.functions.RuleDeserializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;

/**
 * RulesSource 类用于初始化不同类型的数据源，具体支持 Kafka、Pub/Sub 和 Socket 作为数据源。
 * 同时提供将字符串流转换为规则流的方法。
 */
public class RulesSource {

    // 设置规则流的并行度
    private static final int RULES_STREAM_PARALLELISM = 1;

    /**
     * 初始化规则数据源，根据配置选择不同的源（Kafka、Pub/Sub 或 Socket）。
     *
     * @param config 配置信息
     * @param env    Flink 流处理执行环境
     * @return 数据流源
     * @throws IOException 如果在配置或连接源时出现问题
     */
    public static DataStreamSource<String> initRulesSource(
            Config config, StreamExecutionEnvironment env) throws IOException {

        // 获取配置中的规则数据源类型
        String sourceType = config.get(RULES_SOURCE);

        // 根据源类型选择对应的数据源
        RulesSource.Type rulesSourceType = RulesSource.Type.valueOf(sourceType.toUpperCase());
        DataStreamSource<String> dataStreamSource;

        // 根据源类型选择相应的数据源实现
        switch (rulesSourceType) {
            case KAFKA:
                // 为 Kafka 源初始化配置属性
                Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
                String rulesTopic = config.get(RULES_TOPIC);

                // 创建 Kafka 数据源
                KafkaSource<String> kafkaSource =
                        KafkaSource.<String>builder()
                                .setProperties(kafkaProps)  // 设置 Kafka 配置
                                .setTopics(rulesTopic)      // 设置 Kafka 主题
                                .setStartingOffsets(OffsetsInitializer.latest())  // 设置偏移量为最新
                                .setValueOnlyDeserializer(new SimpleStringSchema())  // 设置反序列化方式
                                .build();

                // 从 Kafka 数据源读取数据，未设置水印策略
                dataStreamSource =
                        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Rules Kafka Source");
                break;

            case PUBSUB:
                // 创建 Google Cloud Pub/Sub 数据源
                PubSubSource<String> pubSubSourceFunction =
                        PubSubSource.<String>newBuilder()
                                .withDeserializationSchema(new SimpleStringSchema())  // 设置反序列化方式
                                .withProjectName(config.get(GCP_PROJECT_NAME))  // 设置 GCP 项目名
                                .withSubscriptionName(config.get(GCP_PUBSUB_RULES_SUBSCRIPTION))  // 设置订阅名
                                .build();

                // 从 Pub/Sub 数据源读取数据
                dataStreamSource = env.addSource(pubSubSourceFunction);
                break;

            case SOCKET:
                // 创建 Socket 数据源，连接到指定端口
                SocketTextStreamFunction socketSourceFunction =
                        new SocketTextStreamFunction("localhost", config.get(SOCKET_PORT), "\n", -1);

                // 从 Socket 数据源读取数据
                dataStreamSource = env.addSource(socketSourceFunction);
                break;

            // 如果源类型不匹配，则抛出异常
            default:
                throw new IllegalArgumentException(
                        "Source \"" + rulesSourceType + "\" unknown. Known values are:" + Type.values());
        }
        return dataStreamSource;
    }

    /**
     * 将字符串流转换为规则流。
     *
     * @param ruleStrings 输入的规则字符串流
     * @return 转换后的规则流
     */
    public static DataStream<Rule> stringsStreamToRules(DataStream<String> ruleStrings) {
        return ruleStrings
                .flatMap(new RuleDeserializer())  // 使用 RuleDeserializer 进行反序列化
                .name("Rule Deserialization")  // 设置算子的名称
                .setParallelism(RULES_STREAM_PARALLELISM)  // 设置并行度
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Rule>(Time.of(0, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(Rule element) {
                                // 设置水印提取方式，这里返回最大值以避免水印卡住
                                return Long.MAX_VALUE;
                            }
                        });
    }

    /**
     * 枚举类型，表示支持的数据源类型：Kafka、Pub/Sub 或 Socket。
     */
    public enum Type {
        KAFKA("Rules Source (Kafka)"),
        PUBSUB("Rules Source (Pub/Sub)"),
        SOCKET("Rules Source (Socket)");

        private String name;

        Type(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
