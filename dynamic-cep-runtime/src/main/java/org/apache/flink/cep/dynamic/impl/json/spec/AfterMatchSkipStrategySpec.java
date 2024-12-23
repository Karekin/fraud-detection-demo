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

import org.apache.flink.cep.nfa.aftermatch.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * 用于将 {@link AfterMatchSkipStrategy} 序列化和反序列化为 JSON 格式的工具类。
 *
 * <p>该类支持将不同的跳过策略（AfterMatchSkipStrategy）类型映射为 JSON 字符串，
 * 并从 JSON 数据还原为具体的跳过策略实例。
 */
public class AfterMatchSkipStrategySpec {

    /**
     * 类名与枚举类型的映射关系。
     *
     * <p>用于将策略类名转换为对应的 {@link AfterMatchSkipStrategyType} 枚举值。
     */
    private static final Map<String, String> classToEnumTranslator = new HashMap<>();

    /** 跳过策略的类型。 */
    private final AfterMatchSkipStrategyType type;

    /** 跳过策略的模式名称，可为空。 */
    private final @Nullable String patternName;

    // 静态代码块初始化类名与枚举类型的映射
    static {
        classToEnumTranslator.put(
                NoSkipStrategy.class.getCanonicalName(), AfterMatchSkipStrategyType.NO_SKIP.name());
        classToEnumTranslator.put(
                SkipToNextStrategy.class.getCanonicalName(),
                AfterMatchSkipStrategyType.SKIP_TO_NEXT.name());
        classToEnumTranslator.put(
                SkipPastLastStrategy.class.getCanonicalName(),
                AfterMatchSkipStrategyType.SKIP_PAST_LAST_EVENT.name());
        classToEnumTranslator.put(
                SkipToFirstStrategy.class.getCanonicalName(),
                AfterMatchSkipStrategyType.SKIP_TO_FIRST.name());
        classToEnumTranslator.put(
                SkipToLastStrategy.class.getCanonicalName(),
                AfterMatchSkipStrategyType.SKIP_TO_LAST.name());
    }

    /**
     * 构造方法。
     *
     * <p>通过 JSON 属性初始化跳过策略的类型和模式名称。
     *
     * @param type 跳过策略的类型
     * @param patternName 跳过策略的模式名称，可为空
     */
    public AfterMatchSkipStrategySpec(
            @JsonProperty("type") AfterMatchSkipStrategyType type,
            @JsonProperty("patternName") @Nullable String patternName) {
        this.type = type;
        this.patternName = patternName;
    }

    /**
     * 获取跳过策略的类型。
     *
     * @return 跳过策略类型的枚举值
     */
    public AfterMatchSkipStrategyType getType() {
        return type;
    }

    /**
     * 获取跳过策略的模式名称。
     *
     * @return 模式名称，可能为空
     */
    @Nullable
    public String getPatternName() {
        return patternName;
    }

    /**
     * 从跳过策略实例创建对应的规范对象。
     *
     * @param afterMatchSkipStrategy 跳过策略实例
     * @return 对应的 {@link AfterMatchSkipStrategySpec} 规范对象
     */
    public static AfterMatchSkipStrategySpec fromAfterMatchSkipStrategy(
            AfterMatchSkipStrategy afterMatchSkipStrategy) {
        return new AfterMatchSkipStrategySpec(
                // 根据类名获取对应的枚举值
                AfterMatchSkipStrategyType.valueOf(
                        classToEnumTranslator.get(
                                afterMatchSkipStrategy.getClass().getCanonicalName())),
                // 获取模式名称，如果不存在则返回 null
                afterMatchSkipStrategy.getPatternName().orElse(null));
    }

    /**
     * 将规范对象转换为跳过策略实例。
     *
     * @return 转换后的 {@link AfterMatchSkipStrategy} 实例
     */
    public AfterMatchSkipStrategy toAfterMatchSkipStrategy() {
        switch (this.type) {
            case NO_SKIP:
                return NoSkipStrategy.noSkip();
            case SKIP_TO_LAST:
                return SkipToLastStrategy.skipToLast(this.getPatternName());
            case SKIP_TO_NEXT:
                return SkipToNextStrategy.skipToNext();
            case SKIP_TO_FIRST:
                return SkipToFirstStrategy.skipToFirst(this.getPatternName());
            case SKIP_PAST_LAST_EVENT:
                return SkipPastLastStrategy.skipPastLastEvent();
            default:
                throw new IllegalStateException(
                        "The type of the AfterMatchSkipStrategySpec: "
                                + this.type
                                + " is invalid!");
        }
    }

    /**
     * 跳过策略类型的枚举类，用于序列化和反序列化。
     */
    public enum AfterMatchSkipStrategyType {

        /** 不跳过任何事件。 */
        NO_SKIP(NoSkipStrategy.class.getCanonicalName()),

        /** 跳到下一个事件。 */
        SKIP_TO_NEXT(SkipToNextStrategy.class.getCanonicalName()),

        /** 跳过最后一个事件。 */
        SKIP_PAST_LAST_EVENT(SkipPastLastStrategy.class.getCanonicalName()),

        /** 跳到第一个事件。 */
        SKIP_TO_FIRST(SkipToFirstStrategy.class.getCanonicalName()),

        /** 跳到最后一个事件。 */
        SKIP_TO_LAST(SkipToLastStrategy.class.getCanonicalName());

        /** 对应策略类的完全限定名。 */
        public final String className;

        /**
         * 构造方法。
         *
         * @param className 策略类的完全限定名
         */
        AfterMatchSkipStrategyType(String className) {
            this.className = className;
        }
    }
}
