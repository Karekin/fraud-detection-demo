package org.apache.flink.cep.operator;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.TimeBehaviour;
import org.apache.flink.cep.coordinator.RuleProcessorCoordinatorProvider;
import org.apache.flink.cep.event.EventRecord;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * {@link CepRuleProcessorOperator} 的工厂类。
 *
 * <p>该工厂类负责创建并配置 CepRuleProcessorOperator，支持动态规则处理和时间行为管理。
 * 它实现了多个接口，以支持流式算子的创建、协调器的提供，以及处理时间服务的集成。
 *
 * @param <IN> 输入数据的类型
 * @param <OUT> 输出数据的类型
 *
 * @author shirukai
 */
public class CepRuleProcessorOperatorFactory<IN, OUT> extends AbstractStreamOperatorFactory<OUT>
        implements OneInputStreamOperatorFactory<EventRecord<IN>, OUT>,
        CoordinatedOperatorFactory<OUT>,
        ProcessingTimeServiceAware {

    private static final long serialVersionUID = 6491248798964426467L;

    // 当前规则队列的唯一标识
    private final String ruleQueueId;

    // 用户自定义库的目录路径
    private final String userLibDir;

    // 输入事件比较器，用于处理事件排序等操作
    private final EventComparator<IN> comparator;

    // 用于输出迟到数据的标签
    private final OutputTag<EventRecord<IN>> lateDataOutputTag;

    // 时间行为模式（如处理时间、事件时间）
    private final TimeBehaviour timeBehaviour;

    // 输入数据的序列化器
    private final TypeSerializer<IN> inputSerializer;

    /**
     * 构造函数，用于初始化规则处理算子的工厂。
     *
     * @param inputSerializer 输入数据的序列化器
     * @param ruleQueueId 规则队列的唯一标识
     * @param userLibDir 用户自定义库的目录路径
     * @param timeBehaviour 时间行为（处理时间或事件时间）
     * @param comparator 可选的事件比较器，用于事件排序
     * @param lateDataOutputTag 可选的迟到数据输出标签
     */
    public CepRuleProcessorOperatorFactory(
            final TypeSerializer<IN> inputSerializer,
            String ruleQueueId,
            String userLibDir,
            final TimeBehaviour timeBehaviour,
            @Nullable final EventComparator<IN> comparator,
            @Nullable final OutputTag<EventRecord<IN>> lateDataOutputTag) {

        this.ruleQueueId = ruleQueueId;
        this.userLibDir = userLibDir;
        this.timeBehaviour = timeBehaviour;
        this.comparator = comparator;
        this.lateDataOutputTag = lateDataOutputTag;
        this.inputSerializer = inputSerializer;
    }

    /**
     * 获取算子协调器的提供者。 TODO 背后有什么机制？
     *
     * <p>该方法返回一个协调器提供者，用于管理规则的分发和协调。
     *
     * @param operatorName 算子名称
     * @param operatorID 算子唯一标识
     * @return 算子协调器的提供者
     */
    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new RuleProcessorCoordinatorProvider(operatorName, operatorID, ruleQueueId);
    }

    /**
     * 创建流式算子。
     *
     * <p>该方法根据提供的参数初始化并返回一个 CepRuleProcessorOperator。
     *
     * @param parameters 流式算子的参数
     * @param <T> 返回的流式算子类型
     * @return 已创建的流式算子
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        try {
            // 创建规则处理算子实例
            final CepRuleProcessorOperator<IN, OUT> processorOperator = new CepRuleProcessorOperator<>(
                    processingTimeService,
                    inputSerializer,
                    timeBehaviour == TimeBehaviour.ProcessingTime, // 确定时间行为是否为处理时间
                    comparator,
                    lateDataOutputTag,
                    userLibDir);

            // 设置算子的运行环境
            processorOperator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());

            // 注册事件处理器
            parameters
                    .getOperatorEventDispatcher()
                    .registerEventHandler(operatorId, processorOperator);

            // 返回创建的算子实例
            return (T) processorOperator;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Cannot create udf rule processor operator for "
                            + parameters.getStreamConfig().getOperatorName(),
                    e);
        }
    }

    /**
     * 获取流式算子的类信息。
     *
     * <p>返回与当前算子对应的实现类。
     *
     * @param classLoader 类加载器
     * @return 流式算子的类类型
     */
    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return UdfRuleProcessorOperator.class;
    }
}

