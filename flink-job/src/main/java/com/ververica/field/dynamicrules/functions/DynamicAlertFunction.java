package com.ververica.field.dynamicrules.functions;

import static com.ververica.field.dynamicrules.functions.ProcessingUtils.addToStateValuesSet;
import static com.ververica.field.dynamicrules.functions.ProcessingUtils.handleRuleBroadcast;

import com.ververica.field.dynamicrules.Alert;
import com.ververica.field.dynamicrules.FieldsExtractor;
import com.ververica.field.dynamicrules.Keyed;
import com.ververica.field.dynamicrules.Rule;
import com.ververica.field.dynamicrules.Rule.ControlType;
import com.ververica.field.dynamicrules.Rule.RuleState;
import com.ververica.field.dynamicrules.RuleHelper;
import com.ververica.field.dynamicrules.RulesEvaluator.Descriptors;
import com.ververica.field.dynamicrules.Transaction;

import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 实现动态规则评估和告警逻辑的核心类。
 * - 根据交易事件与广播规则进行评估。
 * - 动态生成告警，并清理过期状态。
 * - 支持控制命令的处理（如清理状态、导出规则等）。
 */
@Slf4j
public class DynamicAlertFunction
        extends KeyedBroadcastProcessFunction<
        String, Keyed<Transaction, String, Integer>, Rule, Alert> {

    // 特殊字段常量，用于识别 COUNT 类型的聚合规则
    private static final String COUNT = "COUNT_FLINK";
    private static final String COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK";

    // 特殊键值，用于标识具有最大窗口的规则和清理状态命令
    private static int WIDEST_RULE_KEY = Integer.MIN_VALUE;
    private static int CLEAR_STATE_COMMAND_KEY = Integer.MIN_VALUE + 1;

    // 用于存储事件的窗口状态
    private transient MapState<Long, Set<Transaction>> windowState;

    // 用于统计每秒告警数量的计量器
    private Meter alertMeter;

    // 定义窗口状态的描述符
    private MapStateDescriptor<Long, Set<Transaction>> windowStateDescriptor =
            new MapStateDescriptor<>(
                    "windowState", // 状态的名称
                    BasicTypeInfo.LONG_TYPE_INFO, // 键的类型
                    TypeInformation.of(new TypeHint<Set<Transaction>>() {
                    }) // 值的类型
            );

    /**
     * 初始化方法，在任务启动时调用。
     * - 初始化窗口状态。
     * - 注册告警计量器。
     */
    @Override
    public void open(Configuration parameters) {
        windowState = getRuntimeContext().getMapState(windowStateDescriptor);
        alertMeter = new MeterView(60); // 每 60 秒更新一次
        getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter); // 注册计量器
    }

    /**
     * 处理每个键控的交易事件，根据规则评估是否需要生成告警。
     *
     * @param value 包含交易事件、键值和规则 ID 的对象
     * @param ctx 上下文，用于访问广播状态和计时器服务
     * @param out 收集器，用于输出告警
     * @throws Exception 如果评估过程中出现问题
     */
    @Override
    public void processElement(
            Keyed<Transaction, String, Integer> value, ReadOnlyContext ctx, Collector<Alert> out)
            throws Exception {
        long currentEventTime = value.getWrapped().getEventTime(); // 当前事件时间
        addToStateValuesSet(windowState, currentEventTime, value.getWrapped()); // 添加事件到窗口状态

        long ingestionTime = value.getWrapped().getIngestionTimestamp();
        ctx.output(Descriptors.latencySinkTag, System.currentTimeMillis() - ingestionTime); // 输出延迟

        Rule rule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(value.getId()); // 获取对应规则

        if (noRuleAvailable(rule)) { // 检查规则是否存在
            log.error("Rule with ID {} does not exist", value.getId());
            return;
        }

        if (rule.getRuleState() == Rule.RuleState.ACTIVE) { // 仅对活动规则进行处理
            Long windowStartForEvent = rule.getWindowStartFor(currentEventTime); // 获取窗口开始时间

            long cleanupTime = (currentEventTime / 1000) * 1000; // 计算清理时间
            ctx.timerService().registerEventTimeTimer(cleanupTime); // 注册事件时间计时器

            // 创建聚合器，根据规则类型进行聚合
            SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
            for (Long stateEventTime : windowState.keys()) {
                if (isStateValueInWindow(stateEventTime, windowStartForEvent, currentEventTime)) {
                    aggregateValuesInState(stateEventTime, aggregator, rule);
                }
            }

            BigDecimal aggregateResult = aggregator.getLocalValue(); // 获取聚合结果
            boolean ruleResult = rule.apply(aggregateResult); // 应用规则逻辑

            // 输出规则评估结果到侧输出流
            ctx.output(
                    Descriptors.demoSinkTag,
                    "Rule "
                            + rule.getRuleId()
                            + " | "
                            + value.getKey()
                            + " : "
                            + aggregateResult.toString()
                            + " -> "
                            + ruleResult);

            if (ruleResult) { // 如果规则被触发，生成告警
                if (COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
                    evictAllStateElements(); // 清空状态
                }
                alertMeter.markEvent(); // 更新告警计量器
                out.collect(new Alert<>(rule.getRuleId(), rule, value.getKey(), value.getWrapped(), aggregateResult));
            }
        }
    }

    /**
     * 处理广播的规则更新。
     * - 根据规则状态进行相应的广播状态更新。
     *
     * @param rule 广播的规则
     * @param ctx 上下文，用于访问广播状态
     * @param out 未使用的收集器
     * @throws Exception 如果广播状态更新失败
     */
    @Override
    public void processBroadcastElement(Rule rule, Context ctx, Collector<Alert> out)
            throws Exception {
        log.info("{}", rule); // 日志记录规则更新
        BroadcastState<Integer, Rule> broadcastState = ctx.getBroadcastState(Descriptors.rulesDescriptor);
        handleRuleBroadcast(rule, broadcastState); // 更新广播状态
        updateWidestWindowRule(rule, broadcastState); // 更新具有最大窗口的规则
        if (rule.getRuleState() == RuleState.CONTROL) {
            handleControlCommand(rule, broadcastState, ctx); // 处理控制命令
        }
    }

    private void handleControlCommand(
            Rule command, BroadcastState<Integer, Rule> rulesState, Context ctx) throws Exception {
        ControlType controlType = command.getControlType();
        switch (controlType) {
            case EXPORT_RULES_CURRENT:
                for (Map.Entry<Integer, Rule> entry : rulesState.entries()) {
                    ctx.output(Descriptors.currentRulesSinkTag, entry.getValue());
                }
                break;
            case CLEAR_STATE_ALL:
                ctx.applyToKeyedState(windowStateDescriptor, (key, state) -> state.clear());
                break;
            case CLEAR_STATE_ALL_STOP:
                rulesState.remove(CLEAR_STATE_COMMAND_KEY);
                break;
            case DELETE_RULES_ALL:
                Iterator<Entry<Integer, Rule>> entriesIterator = rulesState.iterator();
                while (entriesIterator.hasNext()) {
                    Entry<Integer, Rule> ruleEntry = entriesIterator.next();
                    rulesState.remove(ruleEntry.getKey());
                    log.info("Removed Rule {}", ruleEntry.getValue());
                }
                break;
        }
    }

    /**
     * 判断状态中的值是否在窗口范围内。
     *
     * @param stateEventTime 状态中的事件时间
     * @param windowStartForEvent 窗口的开始时间
     * @param currentEventTime 当前事件时间
     * @return 是否在窗口范围内
     */
    private boolean isStateValueInWindow(Long stateEventTime, Long windowStartForEvent, long currentEventTime) {
        return stateEventTime >= windowStartForEvent && stateEventTime <= currentEventTime;
    }

    /**
     * 根据规则和窗口中的事件时间，对状态值进行聚合。
     *
     * @param stateEventTime 状态中的事件时间
     * @param aggregator 聚合器，用于累加值
     * @param rule 当前规则
     * @throws Exception 如果聚合过程中出现错误
     */
    private void aggregateValuesInState(
            Long stateEventTime, SimpleAccumulator<BigDecimal> aggregator, Rule rule) throws Exception {
        Set<Transaction> inWindow = windowState.get(stateEventTime);
        if (COUNT.equals(rule.getAggregateFieldName()) || COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
            for (Transaction event : inWindow) {
                aggregator.add(BigDecimal.ONE); // 对事件数量进行累加
            }
        } else {
            for (Transaction event : inWindow) {
                BigDecimal aggregatedValue =
                        FieldsExtractor.getBigDecimalByName(rule.getAggregateFieldName(), event);
                aggregator.add(aggregatedValue); // 根据规则字段累加值
            }
        }
    }

    /**
     * 检查规则是否可用。
     *
     * @param rule 待检查的规则
     * @return 如果规则不存在或为空，返回 true
     */
    private boolean noRuleAvailable(Rule rule) {
        // This could happen if the BroadcastState in this CoProcessFunction was updated after it was
        // updated and used in `DynamicKeyFunction`
        if (rule == null) {
            return true;
        }
        return false;
    }

    /**
     * 更新具有最大窗口的规则。
     *
     * @param rule 新规则
     * @param broadcastState 广播状态
     * @throws Exception 如果更新过程中出现问题
     */
    private void updateWidestWindowRule(Rule rule, BroadcastState<Integer, Rule> broadcastState)
            throws Exception {
        Rule widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);

        if (rule.getRuleState() != Rule.RuleState.ACTIVE) { // 非活动规则不参与更新
            return;
        }

        if (widestWindowRule == null) {
            broadcastState.put(WIDEST_RULE_KEY, rule);
            return;
        }

        if (widestWindowRule.getWindowMillis() < rule.getWindowMillis()) {
            broadcastState.put(WIDEST_RULE_KEY, rule);
        }
    }

    /**
     * 在定时器触发时清理过期的状态元素。
     *
     * @param timestamp 定时器触发的时间戳
     * @param ctx 上下文，用于访问广播状态
     * @param out 未使用的收集器
     * @throws Exception 如果清理过程中出现问题
     */
    @Override
    public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Alert> out)
            throws Exception {
        Rule widestWindowRule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(WIDEST_RULE_KEY);

        Optional<Long> cleanupEventTimeWindow =
                Optional.ofNullable(widestWindowRule).map(Rule::getWindowMillis);
        Optional<Long> cleanupEventTimeThreshold =
                cleanupEventTimeWindow.map(window -> timestamp - window);

        cleanupEventTimeThreshold.ifPresent(this::evictAgedElementsFromWindow); // 清理过期元素
    }

    /**
     * 清理过期的窗口状态元素。
     *
     * @param threshold 清理的时间阈值
     */
    private void evictAgedElementsFromWindow(Long threshold) {
        try {
            Iterator<Long> keys = windowState.keys().iterator();
            while (keys.hasNext()) {
                Long stateEventTime = keys.next();
                if (stateEventTime < threshold) {
                    keys.remove(); // 移除过期的状态
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * 清空所有状态元素。
     */
    private void evictAllStateElements() {
        try {
            Iterator<Long> keys = windowState.keys().iterator();
            while (keys.hasNext()) {
                keys.next();
                keys.remove(); // 移除所有状态
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
