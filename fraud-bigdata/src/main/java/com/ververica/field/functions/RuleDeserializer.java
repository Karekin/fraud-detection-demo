package com.ververica.field.functions;

import com.ververica.field.model.Rule;
import com.ververica.field.model.Rule.RuleState;
import com.ververica.field.engine.threshold.utils.RuleParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


/**
 * 规则反序列化类，用于将 JSON 字符串解析为 {@link Rule} 对象。
 *
 * <p>该类继承自 {@link RichFlatMapFunction}，在 Flink 数据流处理中，
 * 解析输入的 JSON 字符串为规则对象，并将其输出到下游。
 */
@Slf4j
public class RuleDeserializer extends RichFlatMapFunction<String, Rule> {

    /** 规则解析器，用于将字符串解析为规则对象。 */
    private RuleParser ruleParser;

    /**
     * 在任务初始化时调用，用于初始化规则解析器。
     *
     * <p>通过 Flink 的生命周期方法，设置解析器实例。
     *
     * @param parameters Flink 的配置参数
     * @throws Exception 如果初始化过程中发生错误
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化规则解析器
        ruleParser = new RuleParser();
    }

    /**
     * 核心逻辑，用于将 JSON 字符串解析为规则对象并输出。
     *
     * <p>该方法对输入的每条数据调用，尝试将 JSON 字符串解析为 {@link Rule} 对象。
     * 如果解析成功并通过验证，将规则对象输出到下游；如果解析失败或验证未通过，
     * 记录警告日志并丢弃该条数据。
     *
     * @param value 输入的 JSON 字符串
     * @param out Flink 的收集器，用于将规则对象发送到下游
     * @throws Exception 如果解析或收集过程中发生错误
     */
    @Override
    public void flatMap(String value, Collector<Rule> out) throws Exception {
        // 打印输入的 JSON 字符串到日志中
        log.info("{}", value);
        try {
            // 使用规则解析器解析字符串
            Rule rule = ruleParser.fromString(value);

            // 验证规则对象：如果状态不是 CONTROL 且 ruleId 为空，则抛出异常
            if (rule.getRuleState() != RuleState.CONTROL && rule.getRuleId() == null) {
                throw new NullPointerException("ruleId cannot be null: " + rule.toString());
            }

            // 如果解析成功并验证通过，收集结果并发送到下游
            out.collect(rule);
        } catch (Exception e) {
            // 如果解析失败或验证未通过，记录警告日志并丢弃该条数据
            log.warn("Failed parsing rule, dropping it:", e);
        }
    }
}

