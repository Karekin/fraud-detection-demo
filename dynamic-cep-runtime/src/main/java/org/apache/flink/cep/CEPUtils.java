package org.apache.flink.cep;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.cep.discover.RuleDiscovererFactory;
import org.apache.flink.cep.event.EventRecord;
import org.apache.flink.cep.operator.CepRuleProcessorOperatorFactory;
import org.apache.flink.cep.operator.RuleDistributorOperatorFactory;
import org.apache.flink.cep.operator.UdfRuleProcessorOperatorFactory;
import org.apache.flink.cep.types.RuleRowKey;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * 复杂事件处理的工具类。
 *
 * <p>该类提供的方法将 {@link DataStream} 转换为 {@link PatternStream}，用于复杂事件处理 (CEP) 的实现。
 */
public class CEPUtils {

    /**
     * 动态 UDF 规则处理方法。
     *
     * <p>该方法支持动态加载用户定义的规则 (UDF)，并对输入的数据流进行规则分发和处理。
     *
     * @param input 数据流输入
     * @param discovererFactory 规则发现工厂，用于生成规则发现器
     * @param outTypeInfo 输出数据类型信息
     * @param ruleQueueId 规则队列标识，用于唯一标识当前规则集
     * @param userLibDir 用户自定义库的路径 TODO 研究
     * @param keyBindingEnabled 是否启用键绑定 TODO 研究
     * @param <T> 输入流的数据类型
     * @param <R> 输出流的数据类型
     * @return 处理后的单输出流算子
     */
    public static <T, R> SingleOutputStreamOperator<R> dynamicUdfRules(
            DataStream<T> input,
            RuleDiscovererFactory discovererFactory,
            TypeInformation<R> outTypeInfo,
            String ruleQueueId,
            String userLibDir,
            boolean keyBindingEnabled
    ) {
        // 创建规则分发算子工厂
        final RuleDistributorOperatorFactory<T> distributorOperatorFactory =
                new RuleDistributorOperatorFactory<>(
                        ruleQueueId,
                        discovererFactory,
                        keyBindingEnabled
                );

        // 创建用户定义函数 (UDF) 规则处理算子工厂
        final UdfRuleProcessorOperatorFactory<T, R> processorDiscovererFactory =
                new UdfRuleProcessorOperatorFactory<>(ruleQueueId, userLibDir);

        // 定义事件记录的类型信息
        TypeHint<EventRecord<T>> typeHint = new TypeHint<EventRecord<T>>() {};
        TypeInformation<EventRecord<T>> eventTypeInformation = TypeInformation.of(typeHint);

        // 判断输入流是否为键控流
        if (input instanceof KeyedStream) {
            KeyedStream<T, ?> keyedStream = (KeyedStream<T, ?>) input;
            KeySelector<T, ?> keySelector = keyedStream.getKeySelector();

            // 对键控流应用规则分发算子和处理算子
            return keyedStream.transform("RuleDistributorOperator", eventTypeInformation, distributorOperatorFactory)
                    .keyBy(ruleKeySelector(keySelector))
                    .transform("UdfRuleProcessorOperator", outTypeInfo, processorDiscovererFactory);
        } else {
            // 对非键控流进行规则分发和强制单并行度处理
            return input.keyBy(RuleRowKey.nullRowKeySelector())
                    .transform("RuleDistributorOperator", eventTypeInformation, distributorOperatorFactory)
                    .forceNonParallel() // 强制单线程处理规则分发
                    .keyBy((KeySelector<EventRecord<T>, RuleRowKey<?>>) value -> RuleRowKey.of(value.getRuleId()))
                    .transform("UdfRuleProcessorOperator", outTypeInfo, processorDiscovererFactory);
        }
    }

    /**
     * 动态 CEP 规则处理方法。
     *
     * <p>该方法支持动态加载 CEP 规则，并对输入的数据流进行事件检测和处理。
     *
     * @param input 数据流输入
     * @param discovererFactory 规则发现工厂，用于生成规则发现器
     * @param timeBehaviour 时间行为配置，用于定义时间窗口
     * @param outTypeInfo 输出数据类型信息
     * @param ruleQueueId 规则队列标识，用于唯一标识当前规则集
     * @param userLibDir 用户自定义库的路径
     * @param keyBindingEnabled 是否启用键绑定
     * @param <T> 输入流的数据类型
     * @param <R> 输出流的数据类型
     * @return 处理后的单输出流算子
     */
    public static <T, R> SingleOutputStreamOperator<R> dynamicCepRules(
            DataStream<T> input,
            RuleDiscovererFactory discovererFactory,
            TimeBehaviour timeBehaviour,
            TypeInformation<R> outTypeInfo,
            String ruleQueueId,
            String userLibDir,
            boolean keyBindingEnabled
    ) {
        // 创建规则分发算子工厂
        final RuleDistributorOperatorFactory<T> distributorOperatorFactory =
                new RuleDistributorOperatorFactory<>(
                        ruleQueueId,
                        discovererFactory,
                        keyBindingEnabled
                );

        // 创建 CEP 规则处理算子工厂
        final CepRuleProcessorOperatorFactory<T, R> processorDiscovererFactory =
                new CepRuleProcessorOperatorFactory<>(
                        input.getType().createSerializer(input.getExecutionConfig()),
                        ruleQueueId,
                        userLibDir,
                        timeBehaviour,
                        null,
                        null
                );

        // 定义事件记录的类型信息
        TypeHint<EventRecord<T>> typeHint = new TypeHint<EventRecord<T>>() {};
        TypeInformation<EventRecord<T>> eventTypeInformation = TypeInformation.of(typeHint);

        // 判断输入流是否为键控流
        if (input instanceof KeyedStream) {
            KeyedStream<T, ?> keyedStream = (KeyedStream<T, ?>) input;
            KeySelector<T, ?> keySelector = keyedStream.getKeySelector();

            // 对键控流应用规则分发算子和处理算子
            return keyedStream.transform("RuleDistributorOperator", eventTypeInformation, distributorOperatorFactory)
                    .keyBy(ruleKeySelector(keySelector)) // TODO 如何实现抖音电商的 key generator 逻辑？
                    .transform("CepRuleProcessorOperator", outTypeInfo, processorDiscovererFactory);
        } else {
            // 对非键控流进行规则分发和强制单并行度处理
            return input.keyBy(RuleRowKey.nullRowKeySelector())
                    .transform("RuleDistributorOperator", eventTypeInformation, distributorOperatorFactory)
                    .forceNonParallel()
                    .keyBy((KeySelector<EventRecord<T>, RuleRowKey<?>>) value -> RuleRowKey.of(value.getRuleId()))
                    .transform("CepRuleProcessorOperator", outTypeInfo, processorDiscovererFactory);
        }
    }

    /**
     * 构造规则键选择器。
     *
     * <p>该方法用于为规则的事件记录生成键选择器，用于事件分组。
     *
     * @param keySelector 原始数据的键选择器
     * @param <K> 键的类型
     * @param <T> 数据类型
     * @return 规则键选择器
     */
    public static <K, T> KeySelector<EventRecord<T>, RuleRowKey<K>> ruleKeySelector(KeySelector<T, K> keySelector) {
        return new KeySelector<EventRecord<T>, RuleRowKey<K>>() {
            @Override
            public RuleRowKey<K> getKey(EventRecord<T> value) throws Exception {
                // 获取用户自定义的键，并构造规则键
                K userKey = keySelector.getKey(value.getEvent());
                return RuleRowKey.of(value.getRuleId(), userKey);
            }
        };
    }
}

