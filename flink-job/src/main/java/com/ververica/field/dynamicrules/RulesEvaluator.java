package com.ververica.field.dynamicrules;

import static com.ververica.field.config.Parameters.*;

import com.ververica.field.config.Config;
import com.ververica.field.dynamicrules.functions.*;
import com.ververica.field.dynamicrules.sinks.*;
import com.ververica.field.dynamicrules.sources.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * 动态规则评估器 (RulesEvaluator)
 * - 负责处理交易流和动态规则流，并根据规则生成警报。
 * - 支持使用 Kafka 或 Socket 作为规则源。
 * - 集成了 Flink 的高级功能，例如广播状态、事件时间处理和检查点机制。
 */
@Slf4j
public class RulesEvaluator {

    private final Config config; // 配置对象，用于获取参数。

    RulesEvaluator(Config config) {
        this.config = config;
    }

    /**
     * 执行主要流程，包括环境配置、数据流构建和作业执行。
     */
    public void run() throws Exception {

        RulesSource.Type rulesSourceType = getRulesSourceType(); // 获取规则源类型。
        boolean isLocal = config.get(LOCAL_EXECUTION); // 判断是否为本地执行模式。
        boolean enableCheckpoints = config.get(ENABLE_CHECKPOINTS); // 检查点是否启用。
        int checkpointsInterval = config.get(CHECKPOINT_INTERVAL); // 检查点间隔时间。
        int minPauseBtwnCheckpoints = config.get(MIN_PAUSE_BETWEEN_CHECKPOINTS); // 检查点最小间隔。

        // 配置 Flink 执行环境
        StreamExecutionEnvironment env = configureStreamExecutionEnvironment(rulesSourceType, isLocal);

        if (enableCheckpoints) {
            env.enableCheckpointing(checkpointsInterval); // 启用检查点
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBtwnCheckpoints); // 设置检查点间隔
        }

        // 创建规则更新流和交易流
        DataStream<Rule> rulesUpdateStream = getRulesUpdateStream(env);
        DataStream<Transaction> transactions = getTransactionsStream(env);

        // 广播规则流
        BroadcastStream<Rule> rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor);

        // 处理逻辑：交易流与广播规则流结合处理
        SingleOutputStreamOperator<Alert> alerts = transactions
                .connect(rulesStream)
                .process(new DynamicKeyFunction()) // 动态分区
                .uid("DynamicKeyFunction")
                .name("Dynamic Partitioning Function")
                .keyBy(Keyed::getKey) // 按键分组
                .connect(rulesStream)
                .process(new DynamicAlertFunction()) // 动态规则评估
                .uid("DynamicAlertFunction")
                .name("Dynamic Rule Evaluation Function");

        // 从侧输出流中获取不同类型的数据
        DataStream<String> allRuleEvaluations = alerts.getSideOutput(Descriptors.demoSinkTag); // 规则评估输出
        DataStream<Long> latency = alerts.getSideOutput(Descriptors.latencySinkTag); // 延迟数据输出
        DataStream<Rule> currentRules = alerts.getSideOutput(Descriptors.currentRulesSinkTag); // 当前规则输出

        // 打印警报流到控制台
        alerts.print().name("Alert STDOUT Sink");

        // 打印规则评估流到控制台
        allRuleEvaluations.print().setParallelism(1).name("Rule Evaluation Sink");

        // 转换为 JSON 格式的警报和规则流
        DataStream<String> alertsJson = AlertsSink.alertsStreamToJson(alerts);
        DataStream<String> currentRulesJson = CurrentRulesSink.rulesStreamToJson(currentRules);

        currentRulesJson.print();

        // 将警报流输出到外部接收器
        DataStreamSink<String> alertsSink = AlertsSink.addAlertsSink(config, alertsJson);
        alertsSink.setParallelism(1).name("Alerts JSON Sink");

        // 将当前规则流输出到外部接收器
        DataStreamSink<String> currentRulesSink = CurrentRulesSink.addRulesSink(config, currentRulesJson);
        currentRulesSink.setParallelism(1);

        // 计算并输出延迟信息
        DataStream<String> latencies = latency
                .timeWindowAll(Time.seconds(10))
                .aggregate(new AverageAggregate())
                .map(String::valueOf);

        DataStreamSink<String> latencySink = LatencySink.addLatencySink(config, latencies);
        latencySink.name("Latency Sink");

        // 执行 Flink 作业
        env.execute("Fraud Detection Engine");
    }

    /**
     * 获取交易流，解析交易数据并设置时间戳和水位线。
     *
     * @param env Flink 的流执行环境
     * @return 包含交易数据的 DataStream，带有时间戳和水位线
     */
    private DataStream<Transaction> getTransactionsStream(StreamExecutionEnvironment env) {
        // 从配置中获取交易数据源的并行度
        int sourceParallelism = config.get(SOURCE_PARALLELISM);

        // 初始化交易数据源，生成包含原始交易数据（字符串）的 DataStream
        DataStream<String> transactionsStringsStream = TransactionsSource
                .initTransactionsSource(config, env) // 初始化交易数据源
                .name("Transactions Source") // 设置数据流的名称
                .setParallelism(sourceParallelism); // 设置数据流的并行度

        // 将字符串形式的交易数据流转换为 Transaction 对象流，并分配时间戳和水位线
        return TransactionsSource
                .stringsStreamToTransactions(transactionsStringsStream) // 将字符串转换为 Transaction 对象
                .assignTimestampsAndWatermarks(
                        new SimpleBoundedOutOfOrdernessTimestampExtractor<>(config.get(OUT_OF_ORDERNESS)) // 设置乱序处理的时间戳提取器
                );
    }

    /**
     * 获取规则更新流。
     * 从规则源中读取原始规则数据，将其解析为 `Rule` 对象流。
     *
     * @param env Flink 的流执行环境
     * @return 包含规则数据的 DataStream
     * @throws IOException 如果规则源初始化失败
     */
    private DataStream<Rule> getRulesUpdateStream(StreamExecutionEnvironment env) throws IOException {
        // 获取规则源的类型（如 Kafka 或 Socket），用于动态配置规则数据源
        RulesSource.Type rulesSourceEnumType = getRulesSourceType();

        // 初始化规则数据源，生成包含原始规则数据（字符串）的 DataStream
        DataStream<String> rulesStrings = RulesSource
                .initRulesSource(config, env) // 根据配置初始化规则数据源
                .name(rulesSourceEnumType.getName()) // 设置数据流的名称，与规则源类型匹配
                .setParallelism(1); // 设置并行度为 1，确保规则流的全局一致性

        // 将字符串形式的规则数据流解析为 Rule 对象流
        return RulesSource.stringsStreamToRules(rulesStrings);
    }

    /**
     * 获取规则源类型。
     */
    private RulesSource.Type getRulesSourceType() {
        String rulesSource = config.get(RULES_SOURCE);
        return RulesSource.Type.valueOf(rulesSource.toUpperCase());
    }

    /**
     * 配置 Flink 流执行环境。
     * 根据规则源类型和执行模式（本地或集群）动态设置执行环境。
     *
     * @param rulesSourceEnumType 规则源类型（如 Kafka 或 Socket）
     * @param isLocal 是否为本地执行模式
     * @return 配置完成的 Flink 流执行环境
     */
    private StreamExecutionEnvironment configureStreamExecutionEnvironment(
            RulesSource.Type rulesSourceEnumType, boolean isLocal) {
        // 创建 Flink 配置对象，用于设置运行时选项
        Configuration flinkConfig = new Configuration();
        flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true); // 启用本地 Web UI 监控界面

        // 根据是否为本地执行模式，选择创建本地环境或集群环境
        StreamExecutionEnvironment env = isLocal
                ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig) // 创建带有 Web UI 的本地环境
                : StreamExecutionEnvironment.getExecutionEnvironment(); // 使用集群执行环境

        // 配置时间特性为事件时间（Event Time）
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 配置检查点机制
        env.getCheckpointConfig().setCheckpointInterval(config.get(CHECKPOINT_INTERVAL)); // 设置检查点间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.get(MIN_PAUSE_BETWEEN_CHECKPOINTS)); // 设置检查点最小间隔

        // 配置重启策略，根据规则源类型动态调整
        configureRestartStrategy(env, rulesSourceEnumType);

        // 返回配置完成的流执行环境
        return env;
    }


    /**
     * 配置 Flink 的重启策略。
     * 根据规则源类型 (RulesSource.Type)，动态选择合适的重启策略。
     *
     * @param env Flink 的流执行环境
     * @param rulesSourceEnumType 规则源类型（如 SOCKET 或 KAFKA）
     */
    private void configureRestartStrategy(StreamExecutionEnvironment env, RulesSource.Type rulesSourceEnumType) {
        switch (rulesSourceEnumType) {
            case SOCKET:
                // 针对 SOCKET 规则源，设置固定延迟重启策略
                // - 最大重启次数：10 次
                // - 每次重启之间的延迟：10 秒
                env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                        10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
                break;
            case KAFKA:
                // 针对 KAFKA 规则源，采用默认的无限重启策略（未显式设置）。
                // 无限制重启策略适合 Kafka 等高度可用的流式数据源，确保作业持续运行。
                break;
        }
    }


    /**
     * 简单的时间戳提取器，支持乱序事件。
     * 用于从 `Transaction` 对象中提取事件时间，同时设置乱序容忍时间。
     *
     * @param <T> 继承自 `Transaction` 的数据类型
     */
    private static class SimpleBoundedOutOfOrdernessTimestampExtractor<T extends Transaction>
            extends BoundedOutOfOrdernessTimestampExtractor<T> {

        /**
         * 构造函数，设置乱序容忍时间。
         *
         * @param outOfOrderdnessMillis 乱序容忍时间（单位：毫秒）
         */
        public SimpleBoundedOutOfOrdernessTimestampExtractor(int outOfOrderdnessMillis) {
            // 调用父类构造方法，设置乱序容忍时间
            super(Time.of(outOfOrderdnessMillis, TimeUnit.MILLISECONDS));
        }

        /**
         * 从 `Transaction` 对象中提取事件时间戳。
         *
         * @param element 输入的 `Transaction` 对象
         * @return 提取的事件时间戳
         */
        @Override
        public long extractTimestamp(T element) {
            return element.getEventTime(); // 返回 `Transaction` 的事件时间戳
        }
    }


    /**
     * 用于广播状态和侧输出流的描述符。
     * - 提供规则的广播状态描述符，用于动态规则更新。
     * - 定义多个侧输出流标签，用于不同类型的数据输出。
     */
    public static class Descriptors {

        /**
         * 广播状态的描述符。
         * - 描述符名称为 "rules"。
         * - 使用 Integer 类型作为键，Rule 类型作为值。
         * - 用于动态规则更新，将规则流广播到各个任务实例。
         */
        public static final MapStateDescriptor<Integer, Rule> rulesDescriptor =
                new MapStateDescriptor<>(
                        "rules", // 描述符的唯一名称
                        BasicTypeInfo.INT_TYPE_INFO, // 键类型描述符：Integer 类型
                        TypeInformation.of(Rule.class)); // 值类型描述符：Rule 类型

        /**
         * 规则评估的侧输出流标签。
         * - 标签名称为 "demo-sink"。
         * - 数据类型为 String。
         * - 用于输出规则评估的中间结果，方便调试或展示。
         */
        public static final OutputTag<String> demoSinkTag = new OutputTag<String>("demo-sink") {
        };

        /**
         * 延迟计算的侧输出流标签。
         * - 标签名称为 "latency-sink"。
         * - 数据类型为 Long。
         * - 用于输出延迟指标，便于监控系统性能。
         */
        public static final OutputTag<Long> latencySinkTag = new OutputTag<Long>("latency-sink") {
        };

        /**
         * 当前规则的侧输出流标签。
         * - 标签名称为 "current-rules-sink"。
         * - 数据类型为 Rule。
         * - 用于输出当前生效的规则数据，便于动态规则展示或调试。
         */
        public static final OutputTag<Rule> currentRulesSinkTag =
                new OutputTag<Rule>("current-rules-sink") {
                };
    }

}
