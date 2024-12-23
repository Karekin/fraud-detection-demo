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

import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * 用于描述复杂事件处理（CEP）中的嵌套模式图（Graph）的工具类。
 *
 * <p>该类继承自 {@link NodeSpec}，表示一个组节点（Group Node），其包含嵌套图（Graph）、
 * 节点属性和模式匹配规则。
 *
 * <p>支持将嵌套模式图序列化和反序列化为 JSON 格式。
 */
public class GroupNodeSpec extends NodeSpec {

    /** 嵌套的图，包含节点和边的描述。 */
    private final GraphSpec graph;

    /**
     * 构造方法。
     *
     * <p>通过 JSON 属性初始化组节点的名称、量化器、条件、嵌套图、时间、直到条件、
     * 窗口以及跳过策略。
     *
     * @param name 节点名称
     * @param quantifier 节点的量化器规则
     * @param condition 节点的条件规则
     * @param graph 嵌套的图
     * @param times 节点的时间规则（可选）
     * @param untilCondition 节点的直到条件（可选）
     * @param window 节点的窗口规则
     * @param afterMatchSkipStrategy 节点的跳过策略
     */
    public GroupNodeSpec(
            @JsonProperty("name") String name,
            @JsonProperty("quantifier") QuantifierSpec quantifier,
            @JsonProperty("condition") ConditionSpec condition,
            @JsonProperty("graph") GraphSpec graph,
            @Nullable @JsonProperty("times") TimesSpec times,
            @Nullable @JsonProperty("untilCondition") ConditionSpec untilCondition,
            @JsonProperty("window") WindowSpec window,
            @JsonProperty("afterMatchSkipStrategy") AfterMatchSkipStrategySpec afterMatchSkipStrategy) {
        super(name, quantifier, condition, times, untilCondition, window, afterMatchSkipStrategy, PatternNodeType.COMPOSITE);
        this.graph = graph;
    }

    /**
     * 从模式构建组节点规范。
     *
     * <p>递归处理模式中的嵌套图，并生成对应的组节点规范。
     *
     * @param pattern 要转换的模式
     * @return 构建的 {@link GroupNodeSpec} 实例
     */
    public static GroupNodeSpec fromPattern(Pattern<?, ?> pattern) {
        GraphSpec graph = GraphSpec.fromPattern(((GroupPattern<?, ?>) pattern).getRawPattern());
        return NodeSpec.newBuilder(pattern).graph(graph).buildGroup();
    }

    /**
     * 将 {@link GroupNodeSpec} 转换为模式（{@link Pattern}）。
     *
     * <p>通过递归加载嵌套图中的模式，并将其与组节点的规则组合构建完整模式。
     *
     * @param previous 前一个模式
     * @param consumingStrategy 节点的消费策略
     * @param classLoader 类加载器，用于加载条件
     * @param globalConfiguration 全局配置
     * @return 转换后的 {@link Pattern} 实例
     * @throws Exception 如果在反序列化模式时发生错误
     */
    @Override
    public Pattern<?, ?> toPattern(
            final Pattern<?, ?> previous,
            final Quantifier.ConsumingStrategy consumingStrategy,
            final ClassLoader classLoader,
            final Configuration globalConfiguration) throws Exception {

        // 将嵌套图转换为模式
        Pattern<?, ?> pattern = graph.toPattern(classLoader, globalConfiguration);

        // 使用组节点的规则构建模式
        pattern = buildGroupPattern(consumingStrategy, pattern, previous, previous == null);

        // 处理量化器规则
        processQuantifier(pattern, classLoader, globalConfiguration);

        return pattern;
    }

    /**
     * 构建组模式（GroupPattern）。
     *
     * <p>根据消费策略（ConsumingStrategy）和上下文，决定模式如何与前一个模式连接。
     *
     * @param strategy 消费策略
     * @param currentPattern 当前模式
     * @param prevPattern 前一个模式
     * @param isBeginPattern 是否为起始模式
     * @return 构建的 {@link GroupPattern} 实例
     */
    public static GroupPattern<?, ?> buildGroupPattern(
            Quantifier.ConsumingStrategy strategy,
            Pattern<?, ?> currentPattern,
            Pattern<?, ?> prevPattern,
            boolean isBeginPattern) {
        if (strategy.equals(Quantifier.ConsumingStrategy.STRICT)) {
            if (isBeginPattern) {
                currentPattern = Pattern.begin(currentPattern);
            } else {
                currentPattern = prevPattern.next((Pattern) currentPattern);
            }
        } else if (strategy.equals(Quantifier.ConsumingStrategy.SKIP_TILL_NEXT)) {
            currentPattern = prevPattern.followedBy((Pattern) currentPattern);
        } else if (strategy.equals(Quantifier.ConsumingStrategy.SKIP_TILL_ANY)) {
            currentPattern = prevPattern.followedByAny((Pattern) currentPattern);
        }
        return (GroupPattern<?, ?>) currentPattern;
    }

    /**
     * 获取嵌套图。
     *
     * @return 嵌套的 {@link GraphSpec} 实例
     */
    public GraphSpec getGraph() {
        return graph;
    }
}

