package com.ververica.field.dynamicrules.sources;

import static com.ververica.field.config.Parameters.DATA_TOPIC;
import static com.ververica.field.config.Parameters.RECORDS_PER_SECOND;
import static com.ververica.field.config.Parameters.TRANSACTIONS_SOURCE;

import com.ververica.field.config.Config;
import com.ververica.field.dynamicrules.KafkaUtils;
import com.ververica.field.dynamicrules.Transaction;
import com.ververica.field.dynamicrules.functions.JsonDeserializer;
import com.ververica.field.dynamicrules.functions.JsonGeneratorWrapper;
import com.ververica.field.dynamicrules.functions.TimeStamper;
import com.ververica.field.dynamicrules.functions.TransactionsGenerator;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TransactionsSource 类用于初始化交易数据源，可以选择从 Kafka 或本地生成器生成交易数据。
 */
public class TransactionsSource {

    /**
     * 初始化交易数据源，根据配置选择是从 Kafka 还是从本地生成器生成交易数据。
     *
     * @param config 配置对象，用于获取数据源类型及每秒记录数等配置
     * @param env    Flink 流式执行环境
     * @return 返回一个 DataStreamSource，表示交易数据流
     */
    public static DataStreamSource<String> initTransactionsSource(
            Config config, StreamExecutionEnvironment env) {

        // 获取配置中指定的交易数据源类型（Kafka 或本地生成器）
        String sourceType = config.get(TRANSACTIONS_SOURCE);
        TransactionsSource.Type transactionsSourceType =
                TransactionsSource.Type.valueOf(sourceType.toUpperCase());

        // 获取配置中指定的每秒生成的交易记录数
        int transactionsPerSecond = config.get(RECORDS_PER_SECOND);

        DataStreamSource<String> dataStreamSource;

        // 根据不同的数据源类型选择不同的数据源处理逻辑
        switch (transactionsSourceType) {
            case KAFKA:
                // 从 Kafka 配置初始化消费者属性
                Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
                // 获取 Kafka 的交易数据 topic
                String transactionsTopic = config.get(DATA_TOPIC);

                // Kafka 数据源的初始化
                KafkaSource<String> kafkaSource =
                        KafkaSource.<String>builder()
                                .setProperties(kafkaProps) // 设置 Kafka 配置属性
                                .setTopics(transactionsTopic) // 设置 Kafka topic
                                .setStartingOffsets(OffsetsInitializer.latest()) // 设置从最新偏移量开始读取
                                .setValueOnlyDeserializer(new SimpleStringSchema()) // 设置值的反序列化器
                                .build();

                // 使用 KafkaSource 创建 DataStreamSource
                dataStreamSource =
                        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Rules Kafka Source");
                break;
            default:
                // 如果不是从 Kafka 获取数据，则使用本地生成器生成交易数据
                JsonGeneratorWrapper<Transaction> generatorSource =
                        new JsonGeneratorWrapper<>(new TransactionsGenerator(transactionsPerSecond));
                dataStreamSource = env.addSource(generatorSource); // 将生成器作为数据源添加到环境中
        }
        return dataStreamSource;
    }

    /**
     * 将字符串类型的交易数据流转换为 Transaction 类型的数据流。
     *
     * @param transactionStrings 原始的字符串类型的交易数据流
     * @return 转换后的 Transaction 类型的数据流
     */
    public static DataStream<Transaction> stringsStreamToTransactions(
            DataStream<String> transactionStrings) {
        // 使用 JsonDeserializer 进行反序列化，将 JSON 字符串转换为 Transaction 对象
        return transactionStrings
                .flatMap(new JsonDeserializer<Transaction>(Transaction.class)) // 反序列化
                .returns(Transaction.class)
                .flatMap(new TimeStamper<Transaction>()) // 添加时间戳
                .returns(Transaction.class)
                .name("Transactions Deserialization");
    }

    /**
     * 数据源类型的枚举类，定义了本地生成器和 Kafka 两种数据源类型。
     */
    public enum Type {
        GENERATOR("Transactions Source (generated locally)"), // 本地生成的交易数据源
        KAFKA("Transactions Source (Kafka)"); // 从 Kafka 获取的交易数据源

        private final String name;

        // 构造函数
        Type(String name) {
            this.name = name;
        }

        // 获取数据源类型的名称
        public String getName() {
            return name;
        }
    }
}
