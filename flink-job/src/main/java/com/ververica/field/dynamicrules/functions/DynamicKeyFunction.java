package com.ververica.field.dynamicrules.functions;

import static com.ververica.field.dynamicrules.functions.ProcessingUtils.handleRuleBroadcast;

import com.ververica.field.dynamicrules.Keyed;
import com.ververica.field.dynamicrules.KeysExtractor;
import com.ververica.field.dynamicrules.Rule;
import com.ververica.field.dynamicrules.Rule.ControlType;
import com.ververica.field.dynamicrules.Rule.RuleState;
import com.ververica.field.dynamicrules.RulesEvaluator.Descriptors;
import com.ververica.field.dynamicrules.Transaction;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 动态分区函数 (DynamicKeyFunction)
 * - 基于广播规则对输入数据进行动态分区。
 * - 使用 Flink 的广播状态机制动态更新规则。
 * - 处理规则的控制命令（如删除所有规则）。
 */
@Slf4j
public class DynamicKeyFunction
        extends BroadcastProcessFunction<Transaction, Rule, Keyed<Transaction, String, Integer>> {

    private RuleCounterGauge ruleCounterGauge; // 用于记录活动规则数量的指标

    /**
     * 初始化方法，在作业启动时调用。
     *
     * @param parameters 配置参数
     */
    @Override
    public void open(Configuration parameters) {
        ruleCounterGauge = new RuleCounterGauge(); // 初始化规则计数器
        getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", ruleCounterGauge); // 注册指标
    }

    /**
     * 处理输入的交易事件。
     *
     * @param event 输入的交易数据
     * @param ctx   上下文，提供广播状态访问
     * @param out   收集器，用于发送输出数据
     * @throws Exception 如果处理失败
     */
    @Override
    public void processElement(
            Transaction event, ReadOnlyContext ctx, Collector<Keyed<Transaction, String, Integer>> out)
            throws Exception {
        // 获取广播状态（规则）
        ReadOnlyBroadcastState<Integer, Rule> rulesState =
                ctx.getBroadcastState(Descriptors.rulesDescriptor);

        // 根据每个规则的分组键处理事件
        forkEventForEachGroupingKey(event, rulesState, out);
    }

    /**
     * 根据广播规则对事件进行分组，并输出分组后的数据。
     *
     * @param event      输入的交易数据
     * @param rulesState 只读广播状态，包含所有规则
     * @param out        收集器，用于发送分组后的数据
     * @throws Exception 如果处理失败
     */
    private void forkEventForEachGroupingKey(
            Transaction event,
            ReadOnlyBroadcastState<Integer, Rule> rulesState,
            Collector<Keyed<Transaction, String, Integer>> out)
            throws Exception {
        int ruleCounter = 0; // 计数器，用于记录匹配的规则数量
        for (Map.Entry<Integer, Rule> entry : rulesState.immutableEntries()) {
            final Rule rule = entry.getValue(); // 获取规则
            // 构造分组后的数据并发送
            out.collect(
                    new Keyed<>(
                            event, KeysExtractor.getKey(rule.getGroupingKeyNames(), event), rule.getRuleId()));
            ruleCounter++; // 增加规则计数
        }
        ruleCounterGauge.setValue(ruleCounter); // 更新活动规则的计数器
    }

    /**
     * 处理广播规则的更新。
     *
     * @param rule 新的规则
     * @param ctx  上下文，提供广播状态访问
     * @param out  收集器（未使用）
     * @throws Exception 如果处理失败
     */
    @Override
    public void processBroadcastElement(
            Rule rule, Context ctx, Collector<Keyed<Transaction, String, Integer>> out) throws Exception {
        log.info("{}", rule); // 打印收到的规则
        BroadcastState<Integer, Rule> broadcastState =
                ctx.getBroadcastState(Descriptors.rulesDescriptor);

        // 将新规则应用到广播状态
        handleRuleBroadcast(rule, broadcastState);

        // 如果规则是控制类型，处理控制命令
        if (rule.getRuleState() == RuleState.CONTROL) {
            handleControlCommand(rule.getControlType(), broadcastState);
        }
    }

    /**
     * 处理控制命令，例如删除所有规则。
     *
     * @param controlType 控制命令类型
     * @param rulesState  广播状态，包含所有规则
     * @throws Exception 如果处理失败
     */
    private void handleControlCommand(
            ControlType controlType, BroadcastState<Integer, Rule> rulesState) throws Exception {
        switch (controlType) {
            case DELETE_RULES_ALL:
                // 遍历并删除所有规则
                Iterator<Entry<Integer, Rule>> entriesIterator = rulesState.iterator();
                while (entriesIterator.hasNext()) {
                    Entry<Integer, Rule> ruleEntry = entriesIterator.next();
                    rulesState.remove(ruleEntry.getKey()); // 删除规则
                    log.info("Removed Rule {}", ruleEntry.getValue()); // 记录删除的规则
                }
                break;
        }
    }

    /**
     * 用于记录活动规则数量的指标类。
     */
    private static class RuleCounterGauge implements Gauge<Integer> {

        private int value = 0; // 当前规则数量

        /**
         * 设置规则数量。
         *
         * @param value 活动规则数量
         */
        public void setValue(int value) {
            this.value = value;
        }

        /**
         * 获取当前规则数量。
         *
         * @return 活动规则数量
         */
        @Override
        public Integer getValue() {
            return value;
        }
    }
}
