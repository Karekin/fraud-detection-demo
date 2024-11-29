package com.ververica.field.dynamicrules.sinks;

import static com.ververica.field.config.Parameters.GCP_PROJECT_NAME;
import static com.ververica.field.config.Parameters.GCP_PUBSUB_RULES_SUBSCRIPTION;
import static com.ververica.field.config.Parameters.RULES_EXPORT_SINK;
import static com.ververica.field.config.Parameters.RULES_EXPORT_TOPIC;

import com.ververica.field.config.Config;
import com.ververica.field.dynamicrules.KafkaUtils;
import com.ververica.field.dynamicrules.Rule;
import com.ververica.field.dynamicrules.functions.JsonSerializer;

import java.io.IOException;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink;

/**
 * CurrentRulesSink 类用于将规则数据流发送到不同的输出接收端（Sink），包括 Kafka、Pub/Sub 或标准输出（STDOUT）。
 */
public class CurrentRulesSink {

    /**
     * 根据配置选择不同的输出接收端，将规则数据流发送到指定的接收端。
     *
     * @param config 配置对象，用于获取规则接收端的相关参数
     * @param stream 规则数据流
     * @return 返回一个数据流接收器（DataStreamSink）
     * @throws IOException 如果初始化过程中发生 I/O 错误
     */
    public static DataStreamSink<String> addRulesSink(Config config, DataStream<String> stream)
            throws IOException {

        // 获取配置中指定的规则输出接收端类型（KAFKA、PUBSUB、STDOUT）
        String sinkType = config.get(RULES_EXPORT_SINK);
        CurrentRulesSink.Type currentRulesSinkType =
                CurrentRulesSink.Type.valueOf(sinkType.toUpperCase());
        DataStreamSink<String> dataStreamSink;

        // 根据不同的接收端类型选择不同的实现方式
        switch (currentRulesSinkType) {
            case KAFKA:
                // 初始化 Kafka 生产者配置
                Properties kafkaProps = KafkaUtils.initProducerProperties(config);
                // 获取 Kafka topic
                String rulesExportTopic = config.get(RULES_EXPORT_TOPIC);

                // 创建 KafkaSink，配置相关参数
                KafkaSink<String> kafkaSink =
                        KafkaSink.<String>builder()
                                .setKafkaProducerConfig(kafkaProps) // 设置 Kafka 生产者配置
                                .setRecordSerializer(
                                        KafkaRecordSerializationSchema.builder()
                                                .setTopic(rulesExportTopic) // 设置 Kafka topic
                                                .setValueSerializationSchema(new SimpleStringSchema()) // 设置消息序列化方式
                                                .build())
                                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // 设置交付保证（至少一次）
                                .build();
                // 将数据流接入 KafkaSink
                dataStreamSink = stream.sinkTo(kafkaSink);
                break;

            case PUBSUB:
                // 创建 Pub/Sub Sink，配置相关参数
                PubSubSink<String> pubSubSinkFunction =
                        PubSubSink.<String>newBuilder()
                                .withSerializationSchema(new SimpleStringSchema()) // 设置消息序列化方式
                                .withProjectName(config.get(GCP_PROJECT_NAME)) // 设置 GCP 项目名称
                                .withTopicName(config.get(GCP_PUBSUB_RULES_SUBSCRIPTION)) // 设置 Pub/Sub 订阅主题
                                .build();
                // 将数据流接入 Pub/Sub Sink
                dataStreamSink = stream.addSink(pubSubSinkFunction);
                break;

            case STDOUT:
                // 将数据流输出到标准输出（控制台）
                dataStreamSink = stream.addSink(new PrintSinkFunction<>(true));
                break;

            default:
                // 如果配置的接收端类型不合法，抛出异常
                throw new IllegalArgumentException(
                        "Source \"" + currentRulesSinkType + "\" unknown. Known values are:" + Type.values());
        }
        return dataStreamSink; // 返回数据流接收器
    }

    /**
     * 将规则数据流转换为 JSON 格式的字符串流。
     *
     * @param alerts 规则数据流
     * @return 返回转换为 JSON 格式的字符串流
     */
    public static DataStream<String> rulesStreamToJson(DataStream<Rule> alerts) {
        // 使用 JsonSerializer 将规则对象转换为 JSON 字符串
        return alerts.flatMap(new JsonSerializer<>(Rule.class)).name("Rules Deserialization");
    }

    /**
     * 规则输出接收端类型的枚举类，定义了 KAFKA、PUBSUB 和 STDOUT 三种类型。
     */
    public enum Type {
        KAFKA("Current Rules Sink (Kafka)"), // Kafka 输出接收端
        PUBSUB("Current Rules Sink (Pub/Sub)"), // Pub/Sub 输出接收端
        STDOUT("Current Rules Sink (Std. Out)"); // 标准输出（STDOUT）

        private String name;

        // 构造函数
        Type(String name) {
            this.name = name;
        }

        // 获取输出接收端类型的名称
        public String getName() {
            return name;
        }
    }
}
