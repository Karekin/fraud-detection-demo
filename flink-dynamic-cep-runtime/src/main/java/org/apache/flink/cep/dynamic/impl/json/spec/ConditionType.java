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

import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import java.util.HashMap;
import java.util.Map;

/**
 * 动态 CEP 使用的 {@link IterativeCondition} 类型枚举。
 *
 * <p>该枚举定义了动态复杂事件处理（CEP）中条件的类型，并提供了基于字符串类型的映射和查询功能。
 */
public enum ConditionType {

    /**
     * 使用类名定义的条件类型。
     *
     * <p>该类型表示条件是通过 Java 类的完全限定名（Fully Qualified Name）动态加载的。
     */
    CLASS("CLASS"),

    /**
     * 使用 Aviator 表达式定义的条件类型。
     *
     * <p>该类型表示条件通过 Aviator 表达式动态定义和解析。
     */
    AVIATOR("AVIATOR");

    /**
     * 条件类型的字符串表示。
     */
    private final String type;

    /**
     * 条件类型的静态映射，用于从字符串快速查找对应的枚举值。
     */
    private static final Map<String, ConditionType> TYPE_MAP;

    /**
     * 枚举的构造方法。
     *
     * @param type 条件类型的字符串表示
     */
    ConditionType(String type) {
        this.type = type;
    }

    // 静态代码块，用于初始化类型映射
    static {
        TYPE_MAP = new HashMap<>();
        for (ConditionType instance : ConditionType.values()) {
            // 将每个枚举值的字符串表示与枚举值关联
            TYPE_MAP.put(instance.type, instance);
        }
    }

    /**
     * 根据字符串获取对应的条件类型。
     *
     * <p>通过静态映射表快速查找对应的 {@link ConditionType} 枚举值。
     *
     * @param type 条件类型的字符串表示
     * @return 对应的 {@link ConditionType} 枚举值，如果不存在则返回 null
     */
    public static ConditionType get(String type) {
        return TYPE_MAP.get(type);
    }
}
