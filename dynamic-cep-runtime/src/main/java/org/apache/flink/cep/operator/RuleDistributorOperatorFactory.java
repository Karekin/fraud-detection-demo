package org.apache.flink.cep.operator;


import org.apache.flink.cep.coordinator.RuleDistributorCoordinatorProvider;
import org.apache.flink.cep.discover.RuleDiscovererFactory;
import org.apache.flink.cep.event.EventRecord;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;

/**
 * {@link RuleDistributorOperator} 的工厂类。
 *
 * <p>该工厂类用于创建和配置规则分发算子 (RuleDistributorOperator)，
 * 提供规则发现功能，并支持键绑定配置。TODO
 *
 * @param <IN> 输入数据的类型
 *
 * @see AbstractStreamOperatorFactory
 * @see OneInputStreamOperatorFactory
 * @see CoordinatedOperatorFactory
 * @see ProcessingTimeServiceAware
 *
 */
public class RuleDistributorOperatorFactory<IN> extends AbstractStreamOperatorFactory<EventRecord<IN>>
        implements OneInputStreamOperatorFactory<IN, EventRecord<IN>>,
        CoordinatedOperatorFactory<EventRecord<IN>>,
        ProcessingTimeServiceAware {

    // 规则发现工厂，用于创建规则发现器
    private final RuleDiscovererFactory discoverFactory;

    // 规则队列的唯一标识符
    private final String ruleQueueId;

    // 是否启用键绑定
    private final boolean keyBindingEnabled;

    /**
     * 构造函数，用于初始化规则分发算子的工厂。
     *
     * @param ruleQueueId 规则队列的唯一标识符
     * @param discoverFactory 规则发现工厂
     * @param keyBindingEnabled 是否启用键绑定
     */
    public RuleDistributorOperatorFactory(String ruleQueueId, RuleDiscovererFactory discoverFactory, boolean keyBindingEnabled) {
        this.discoverFactory = discoverFactory;
        this.ruleQueueId = ruleQueueId;
        this.keyBindingEnabled = keyBindingEnabled;
    }

    /**
     * 获取算子协调器的提供者。
     *
     * <p>该方法返回一个协调器提供者，用于管理规则的分发和协调。
     *
     * @param operatorName 算子名称
     * @param operatorID 算子的唯一标识符
     * @return 算子协调器的提供者
     */
    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        // 返回规则分发协调器的提供者
        return new RuleDistributorCoordinatorProvider(operatorName, operatorID, ruleQueueId, discoverFactory);
    }

    /**
     * 创建规则分发算子。
     *
     * <p>该方法根据提供的参数初始化并返回一个 RuleDistributorOperator 实例。
     *
     * @param parameters 流式算子的参数
     * @param <T> 返回的流式算子类型
     * @return 已创建的流式算子
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<EventRecord<IN>>> T createStreamOperator(StreamOperatorParameters<EventRecord<IN>> parameters) {
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        try {
            // 创建规则分发算子实例
            final RuleDistributorOperator<IN> distributorOperator = new RuleDistributorOperator<>(processingTimeService, keyBindingEnabled);

            // 设置算子的运行环境
            distributorOperator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());

            // 注册事件处理器，用于处理规则更新事件
            parameters
                    .getOperatorEventDispatcher()
                    .registerEventHandler(operatorId, distributorOperator);

            // 返回已创建的算子实例
            return (T) distributorOperator;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Cannot create rule distributor operator for "
                            + parameters.getStreamConfig().getOperatorName(),
                    e);
        }
    }

    /**
     * 获取规则分发算子的类信息。
     *
     * <p>返回与当前算子对应的实现类。
     *
     * @param classLoader 类加载器
     * @return 规则分发算子的类类型
     */
    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return RuleDistributorOperator.class;
    }
}
