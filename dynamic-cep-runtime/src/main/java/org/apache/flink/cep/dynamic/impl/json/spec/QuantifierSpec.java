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

import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.cep.pattern.Quantifier.QuantifierProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.EnumSet;

/**
 * 用于将 {@link Quantifier} 序列化和反序列化为 JSON 格式的工具类。
 *
 * <p>该类包含 {@link Quantifier} 的核心字段，包括匹配策略（consumingStrategy）、
 * 内部匹配策略（innerConsumingStrategy）以及量化器的属性（properties）。
 * 它还支持将相关逻辑（如 Times 和 untilCondition）一并序列化和反序列化。
 */
public class QuantifierSpec {

    /**
     * 消费策略，定义事件匹配的方式。
     *
     * <p>例如 SKIP_TO_NEXT、SKIP_TILL_NEXT 等策略。
     */
    private final ConsumingStrategy consumingStrategy;

    /**
     * 内部消费策略，用于嵌套模式的事件匹配方式。
     *
     * <p>默认值为 {@link ConsumingStrategy#SKIP_TILL_NEXT}。
     */
    private ConsumingStrategy innerConsumingStrategy = ConsumingStrategy.SKIP_TILL_NEXT;

    /**
     * 量化器的属性集合。
     *
     * <p>使用 {@link EnumSet} 表示，例如贪婪匹配（Greedy）、严格模式（Strict）等。
     */
    private final EnumSet<QuantifierProperty> properties;

    /**
     * 参数化构造方法。
     *
     * <p>通过 JSON 属性初始化量化器的消费策略、内部消费策略和属性集合。
     *
     * @param consumingStrategy 消费策略
     * @param innerConsumingStrategy 内部消费策略
     * @param properties 量化器属性集合
     */
    public QuantifierSpec(
            @JsonProperty("consumingStrategy") ConsumingStrategy consumingStrategy,
            @JsonProperty("innerConsumingStrategy") ConsumingStrategy innerConsumingStrategy,
            @JsonProperty("properties") EnumSet<QuantifierProperty> properties) {
        this.consumingStrategy = consumingStrategy; // 初始化消费策略
        this.properties = properties; // 初始化属性集合
        this.innerConsumingStrategy = innerConsumingStrategy; // 初始化内部消费策略
    }

    /**
     * 从 {@link Quantifier} 实例构造量化器规范。
     *
     * <p>通过提取量化器的消费策略、内部消费策略和属性集合，创建对应的规范对象。
     *
     * @param quantifier {@link Quantifier} 实例
     */
    public QuantifierSpec(Quantifier quantifier) {
        // 提取内部消费策略
        this.innerConsumingStrategy = quantifier.getInnerConsumingStrategy();

        // 提取消费策略
        this.consumingStrategy = quantifier.getConsumingStrategy();

        // 初始化属性集合并提取属性
        this.properties = EnumSet.noneOf(QuantifierProperty.class);
        for (QuantifierProperty property : QuantifierProperty.values()) {
            if (quantifier.hasProperty(property)) {
                this.properties.add(property);
            }
        }
    }

    /**
     * 获取内部消费策略。
     *
     * @return 内部消费策略
     */
    public ConsumingStrategy getInnerConsumingStrategy() {
        return innerConsumingStrategy;
    }

    /**
     * 获取消费策略。
     *
     * @return 消费策略
     */
    public ConsumingStrategy getConsumingStrategy() {
        return consumingStrategy;
    }

    /**
     * 获取量化器的属性集合。
     *
     * @return 属性集合
     */
    public EnumSet<QuantifierProperty> getProperties() {
        return properties;
    }
}
