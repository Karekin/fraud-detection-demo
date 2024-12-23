/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.dynamic.impl.json.spec;

import org.apache.flink.cep.dynamic.PatternWrapper;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.cep.pattern.Quantifier.QuantifierProperty;
import org.apache.flink.cep.pattern.WithinType;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichOrCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * 表示复杂事件处理（CEP）中节点的类。
 *
 * <p>节点（Node）描述了模式包装器（PatternWrapper）的相关字段，支持序列化和反序列化为 JSON 格式。
 * 一个节点可以是基本节点（Atomic）或嵌套的图（Composite）。
 */
public class NodeSpec {

    /** 节点名称，用于标识节点。 */
    private final String name;

    /** 节点的量化器规则，用于定义匹配的频率和方式。 */
    private final QuantifierSpec quantifier;

    /** 节点的匹配条件规则。 */
    private final ConditionSpec condition;

    /** 节点的类型，可以是 ATOMIC 或 COMPOSITE。 */
    private final PatternNodeType type;

    /** 节点的匹配后跳过策略。 */
    private final AfterMatchSkipStrategySpec afterMatchSkipStrategy;

    /** 节点的窗口规则，用于限定匹配时间范围。 */
    protected final WindowSpec window;

    /** 节点的匹配次数规则（可选）。 */
    private final @Nullable TimesSpec times;

    /** 节点的直到条件规则（可选）。 */
    private final @Nullable ConditionSpec untilCondition;

    /**
     * 构造方法。
     *
     * <p>初始化节点的基本属性，包括名称、量化器、条件、匹配次数规则、直到条件、窗口和匹配后跳过策略。
     *
     * @param name 节点名称
     * @param quantifier 节点的量化器规则
     * @param condition 节点的匹配条件规则
     * @param times 节点的匹配次数规则（可选）
     * @param untilCondition 节点的直到条件规则（可选）
     * @param window 节点的窗口规则
     * @param afterMatchStrategy 节点的匹配后跳过策略
     */
    public NodeSpec(
            String name,
            QuantifierSpec quantifier,
            ConditionSpec condition,
            TimesSpec times,
            ConditionSpec untilCondition,
            WindowSpec window,
            AfterMatchSkipStrategySpec afterMatchStrategy) {
        this(name, quantifier, condition, times, untilCondition, window, afterMatchStrategy, PatternNodeType.ATOMIC);
    }

    /**
     * 参数化构造方法。
     *
     * <p>通过 JSON 属性反序列化节点的所有字段。
     *
     * @param name 节点名称
     * @param quantifier 节点的量化器规则
     * @param condition 节点的匹配条件规则
     * @param times 节点的匹配次数规则（可选）
     * @param untilCondition 节点的直到条件规则（可选）
     * @param window 节点的窗口规则
     * @param afterMatchSkipStrategy 节点的匹配后跳过策略
     * @param type 节点的类型
     */
    public NodeSpec(
            @JsonProperty("name") String name,
            @JsonProperty("quantifier") QuantifierSpec quantifier,
            @JsonProperty("condition") ConditionSpec condition,
            @Nullable @JsonProperty("times") TimesSpec times,
            @Nullable @JsonProperty("untilCondition") ConditionSpec untilCondition,
            @JsonProperty("window") WindowSpec window,
            @JsonProperty("afterMatchSkipStrategy") AfterMatchSkipStrategySpec afterMatchSkipStrategy,
            @JsonProperty("type") PatternNodeType type) {
        this.name = name;
        this.quantifier = quantifier;
        this.condition = condition;
        this.times = times;
        this.untilCondition = untilCondition;
        this.window = window;
        this.afterMatchSkipStrategy = afterMatchSkipStrategy;
        this.type = type;
    }


    /**
     * 构建一个新的 {@link Builder} 实例，用于从给定的模式构建节点。
     *
     * @param pattern 要转换的模式
     * @return 用于构建节点的 {@link Builder} 实例
     */
    public static Builder newBuilder(Pattern<?, ?> pattern) {
        // 节点名称
        String name = pattern.getName();

        // 量化器规则
        QuantifierSpec quantifier = new QuantifierSpec(pattern.getQuantifier());

        // 匹配条件
        ConditionSpec condition =
                pattern.getCondition() != null
                        ? ConditionSpec.fromCondition(pattern.getCondition())
                        : null;

        // 窗口规则
        Map<WithinType, Time> windowTime = new HashMap<>();
        if (pattern.getWindowTime(WithinType.FIRST_AND_LAST) != null) {
            windowTime.put(WithinType.FIRST_AND_LAST, pattern.getWindowTime(WithinType.FIRST_AND_LAST));
        } else if (pattern.getWindowTime(WithinType.PREVIOUS_AND_CURRENT) != null) {
            windowTime.put(
                    WithinType.PREVIOUS_AND_CURRENT,
                    pattern.getWindowTime(WithinType.PREVIOUS_AND_CURRENT));
        }
        WindowSpec window = windowTime.isEmpty() ? null : WindowSpec.fromWindowTime(windowTime);

        // 匹配次数规则
        TimesSpec times =
                pattern.getTimes() != null
                        ? TimesSpec.of(pattern.getTimes())
                        : null;

        // 直到条件规则
        ConditionSpec untilCondition =
                pattern.getUntilCondition() != null
                        ? ConditionSpec.fromCondition(pattern.getUntilCondition())
                        : null;

        // 跳过策略
        AfterMatchSkipStrategySpec afterMatchSkipStrategy =
                AfterMatchSkipStrategySpec.fromAfterMatchSkipStrategy(pattern.getAfterMatchSkipStrategy());

        // 构建器
        return new Builder()
                .name(name)
                .quantifier(quantifier)
                .times(times)
                .condition(condition)
                .untilCondition(untilCondition)
                .window(window)
                .afterMatchSkipStrategy(afterMatchSkipStrategy);
    }

    /**
     * 从给定的模式构建一个节点规范（NodeSpec）。
     *
     * @param pattern 要转换的模式
     * @return 转换后的 {@link NodeSpec} 实例
     */
    public static NodeSpec fromPattern(Pattern<?, ?> pattern) {
        return NodeSpec.newBuilder(pattern).buildNode();
    }

    /**
     * 将 {@link NodeSpec} 转换为模式（{@link Pattern}）。
     *
     * <p>基于当前节点的属性（如条件、量化器规则等），生成对应的模式实例。
     *
     * @param previous 前一个模式
     * @param consumingStrategy 当前节点的消费策略
     * @param classLoader 用于加载条件的类加载器
     * @param globalConfiguration 全局配置，用于传递运行时参数
     * @return 转换后的 {@link Pattern} 实例
     * @throws Exception 如果在反序列化模式时发生错误
     */
    public Pattern<?, ?> toPattern(
            final Pattern<?, ?> previous,
            final ConsumingStrategy consumingStrategy,
            final ClassLoader classLoader,
            final Configuration globalConfiguration
    ) throws Exception {
        // 创建一个新的 PatternWrapper
        Pattern<?, ?> pattern = new PatternWrapper(
                this.getName(),
                previous,
                consumingStrategy,
                afterMatchSkipStrategy.toAfterMatchSkipStrategy());

        // 设置匹配条件
        final ConditionSpec conditionSpec = this.getCondition();
        if (conditionSpec != null) {
            IterativeCondition iterativeCondition = conditionSpec.toIterativeCondition(classLoader, globalConfiguration);

            // 如果是 OR 条件，调用 or 方法；否则调用 where 方法
            if (iterativeCondition instanceof RichOrCondition) {
                pattern.or(iterativeCondition);
            } else {
                pattern.where(iterativeCondition);
            }
        }

        // 处理量化器规则
        processQuantifier(pattern, classLoader, globalConfiguration);

        return pattern;
    }

    /**
     * 处理量化器规则并应用到模式。
     *
     * <p>根据量化器的属性和规则配置模式的匹配行为，例如设置匹配次数、贪婪模式等。
     *
     * @param pattern 要应用规则的模式
     * @param classLoader 用于加载条件的类加载器
     * @param globalConfiguration 全局配置，用于运行时参数传递
     * @throws Exception 如果在处理过程中发生错误
     */
    public void processQuantifier(
            Pattern<?, ?> pattern,
            ClassLoader classLoader,
            Configuration globalConfiguration) throws Exception {
        // 应用量化器的属性
        for (QuantifierProperty property : this.getQuantifier().getProperties()) {
            if (property.equals(QuantifierProperty.OPTIONAL)) {
                pattern.optional(); // 设置为可选
            } else if (property.equals(QuantifierProperty.GREEDY)) {
                pattern.greedy(); // 设置为贪婪模式
            } else if (property.equals(QuantifierProperty.LOOPING)) {
                // 如果是循环匹配，设置匹配次数
                final TimesSpec times = this.getTimes();
                if (times != null) {
                    TimesSpec.TimeSpec windowTime = times.getWindowTime();
                    pattern.timesOrMore(
                            times.getFrom(),
                            windowTime != null ? windowTime.toTime() : null);
                }
            } else if (property.equals(QuantifierProperty.TIMES)) {
                // 设置具体匹配次数范围
                final TimesSpec times = this.getTimes();
                if (times != null) {
                    pattern.times(times.getFrom(), times.getTo());
                }
            }
        }

        // 处理量化器的内部消费策略
        final ConsumingStrategy innerConsumingStrategy = this.getQuantifier().getInnerConsumingStrategy();
        if (innerConsumingStrategy.equals(ConsumingStrategy.SKIP_TILL_ANY)) {
            pattern.allowCombinations(); // 允许组合匹配
        } else if (innerConsumingStrategy.equals(ConsumingStrategy.STRICT)) {
            pattern.consecutive(); // 设置为严格连续匹配
        }

        // 设置直到条件规则
        final ConditionSpec untilCondition = this.getUntilCondition();
        if (untilCondition != null) {
            final IterativeCondition iterativeCondition = untilCondition.toIterativeCondition(classLoader, globalConfiguration);
            pattern.until(iterativeCondition);
        }

        // 设置窗口规则
        if (window != null) {
            pattern.within(this.window.getTime(), this.window.getType());
        }
    }


    /**
     * 获取节点的名称。
     *
     * @return 节点名称
     */
    public String getName() {
        return name;
    }

    /**
     * 获取节点的类型。
     *
     * @return 节点类型，可能是 ATOMIC 或 COMPOSITE
     */
    public PatternNodeType getType() {
        return type;
    }

    /**
     * 获取节点的匹配后跳过策略。
     *
     * @return 匹配后跳过策略的规范对象
     */
    public AfterMatchSkipStrategySpec getAfterMatchSkipStrategy() {
        return afterMatchSkipStrategy;
    }

    /**
     * 获取节点的窗口规则。
     *
     * @return 节点的窗口规则
     */
    public WindowSpec getWindow() {
        return window;
    }

    /**
     * 获取节点的量化器规则。
     *
     * @return 节点的量化器规则
     */
    public QuantifierSpec getQuantifier() {
        return quantifier;
    }

    /**
     * 获取节点的匹配条件规则。
     *
     * @return 节点的匹配条件规则
     */
    public ConditionSpec getCondition() {
        return condition;
    }

    /**
     * 获取节点的匹配次数规则。
     *
     * @return 节点的匹配次数规则，可能为空
     */
    @Nullable
    public TimesSpec getTimes() {
        return times;
    }

    /**
     * 获取节点的直到条件规则。
     *
     * @return 节点的直到条件规则，可能为空
     */
    @Nullable
    public ConditionSpec getUntilCondition() {
        return untilCondition;
    }


    /**
     * 表示节点的类型。
     *
     * <p>节点类型可以是以下两种：
     * <ul>
     *   <li>ATOMIC：基本模式包装器节点。
     *   <li>COMPOSITE：包含嵌套图的复杂节点。
     * </ul>
     */
    public enum PatternNodeType {
        /** 表示基本模式包装器节点。 */
        ATOMIC,
        /** 表示包含嵌套图的复杂节点。 */
        COMPOSITE
    }


    /**
     * 构建器类，用于创建 {@link NodeSpec} 或 {@link GroupNodeSpec}。
     *
     * <p>该类提供了链式调用的方式逐步构建节点，支持设置节点的属性。
     */
    public static final class Builder {

        /** 节点的名称。 */
        private String name;

        /** 节点的量化器规则。 */
        private QuantifierSpec quantifier;

        /** 节点的匹配条件规则。 */
        private ConditionSpec condition;

        /** 节点的嵌套图（仅适用于组节点）。 */
        private GraphSpec graph;

        /** 节点的窗口规则。 */
        private WindowSpec window;

        /** 节点的匹配次数规则（可选）。 */
        private @Nullable TimesSpec times;

        /** 节点的直到条件规则（可选）。 */
        private @Nullable ConditionSpec untilCondition;

        /** 节点的匹配后跳过策略。 */
        private AfterMatchSkipStrategySpec afterMatchSkipStrategy;

        /** 默认构造方法。 */
        private Builder() {}

        /**
         * 设置节点名称。
         *
         * @param name 节点名称
         * @return 当前构建器实例
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * 设置量化器规则。
         *
         * @param quantifier 节点的量化器规则
         * @return 当前构建器实例
         */
        public Builder quantifier(QuantifierSpec quantifier) {
            this.quantifier = quantifier;
            return this;
        }

        /**
         * 设置嵌套图。
         *
         * @param graph 节点的嵌套图
         * @return 当前构建器实例
         */
        public Builder graph(GraphSpec graph) {
            this.graph = graph;
            return this;
        }

        /**
         * 设置匹配条件规则。
         *
         * @param condition 节点的匹配条件规则
         * @return 当前构建器实例
         */
        public Builder condition(ConditionSpec condition) {
            this.condition = condition;
            return this;
        }

        /**
         * 设置窗口规则。
         *
         * @param window 节点的窗口规则
         * @return 当前构建器实例
         */
        public Builder window(WindowSpec window) {
            this.window = window;
            return this;
        }

        /**
         * 设置匹配次数规则。
         *
         * @param times 节点的匹配次数规则
         * @return 当前构建器实例
         */
        public Builder times(TimesSpec times) {
            this.times = times;
            return this;
        }

        /**
         * 设置直到条件规则。
         *
         * @param untilCondition 节点的直到条件规则
         * @return 当前构建器实例
         */
        public Builder untilCondition(ConditionSpec untilCondition) {
            this.untilCondition = untilCondition;
            return this;
        }

        /**
         * 设置匹配后跳过策略。
         *
         * @param afterMatchSkipStrategy 节点的匹配后跳过策略
         * @return 当前构建器实例
         */
        public Builder afterMatchSkipStrategy(AfterMatchSkipStrategySpec afterMatchSkipStrategy) {
            this.afterMatchSkipStrategy = afterMatchSkipStrategy;
            return this;
        }

        /**
         * 构建一个普通节点。
         *
         * @return 构建的 {@link NodeSpec} 实例
         */
        public NodeSpec buildNode() {
            return new NodeSpec(this.name, this.quantifier, this.condition, times, untilCondition, window, afterMatchSkipStrategy);
        }

        /**
         * 构建一个组节点。
         *
         * @return 构建的 {@link GroupNodeSpec} 实例
         */
        public GroupNodeSpec buildGroup() {
            return new GroupNodeSpec(name, quantifier, null, graph, times, untilCondition, window, afterMatchSkipStrategy);
        }
    }
}
