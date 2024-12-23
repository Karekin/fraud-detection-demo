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

import org.apache.flink.cep.dynamic.condition.AviatorCondition;
import org.apache.flink.cep.dynamic.condition.CustomArgsCondition;
import org.apache.flink.cep.pattern.conditions.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * 用于序列化和反序列化特定类的 {@link IterativeCondition} 为 JSON 格式的工具类。
 *
 * <p>该类为条件规范的抽象类，支持多种条件类型的处理和动态转换。
 * 不同的条件类型需要通过具体子类实现其序列化和反序列化逻辑。
 */
public abstract class ConditionSpec {

    /**
     * 条件的类型。
     *
     * <p>通过 {@link ConditionType} 表示条件的类别，例如 AVIATOR、CLASS 等。
     */
    private final ConditionType type;

    /**
     * 构造方法。
     *
     * <p>初始化条件类型。
     *
     * @param type 条件的类型，使用 JSON 属性注解以支持反序列化
     */
    ConditionSpec(@JsonProperty("type") ConditionType type) {
        this.type = type;
    }

    /**
     * 从 {@link IterativeCondition} 创建对应的条件规范实例。
     *
     * <p>根据条件的具体类型，动态选择对应的 {@link ConditionSpec} 子类实例。
     * 支持的条件类型包括：
     * <ul>
     *   <li>{@link SubtypeCondition}：子类型条件
     *   <li>{@link AviatorCondition}：基于 Aviator 表达式的条件
     *   <li>{@link CustomArgsCondition}：带有自定义参数的条件
     *   <li>{@link RichCompositeIterativeCondition}：复合条件（AND、OR、NOT）
     *   <li>其他类型的条件则默认使用 {@link ClassConditionSpec}。
     * </ul>
     *
     * @param condition 条件实例
     * @return 对应的条件规范实例
     */
    public static ConditionSpec fromCondition(IterativeCondition<?> condition) {
        // 处理子类型条件
        if (condition instanceof SubtypeCondition) {
            return new SubTypeConditionSpec(condition);

            // 处理 Aviator 表达式条件
        } else if (condition instanceof AviatorCondition) {
            return new AviatorConditionSpec(((AviatorCondition<?>) condition).getExpression());

            // 处理自定义参数条件
        } else if (condition instanceof CustomArgsCondition) {
            return new CustomArgsConditionSpec((CustomArgsCondition<?>) condition);

            // 处理复合条件
        } else if (condition instanceof RichCompositeIterativeCondition) {
            IterativeCondition<?>[] nestedConditions =
                    ((RichCompositeIterativeCondition<?>) condition).getNestedConditions();

            // 处理 OR 条件
            if (condition instanceof RichOrCondition) {
                List<ConditionSpec> specs = new ArrayList<>();
                for (IterativeCondition<?> nestedCondition : nestedConditions) {
                    specs.add(fromCondition(nestedCondition));
                }
                return new RichOrConditionSpec(specs);

                // 处理 AND 条件
            } else if (condition instanceof RichAndCondition) {
                List<ConditionSpec> specs = new ArrayList<>();
                for (IterativeCondition<?> nestedCondition : nestedConditions) {
                    specs.add(fromCondition(nestedCondition));
                }
                return new RichAndConditionSpec(specs);

                // 处理 NOT 条件
            } else if (condition instanceof RichNotCondition) {
                List<ConditionSpec> specs = new ArrayList<>();
                for (IterativeCondition<?> nestedCondition : nestedConditions) {
                    specs.add(fromCondition(nestedCondition));
                }
                return new RichNotConditionSpec(specs);
            }
        }

        // 其他类型的条件默认使用 ClassConditionSpec
        return new ClassConditionSpec(condition);
    }

    /**
     * 将条件规范转换为 {@link IterativeCondition}。
     *
     * <p>通过反射加载对应条件的实现类，并根据全局配置动态创建实例。
     * 具体实现由子类定义。
     *
     * @param classLoader         用于加载条件类的类加载器
     * @param globalConfiguration 全局配置，用于传递参数
     * @return 转换后的 {@link IterativeCondition} 实例
     * @throws Exception 如果转换失败
     */
    public abstract IterativeCondition<?> toIterativeCondition(
            ClassLoader classLoader,
            Configuration globalConfiguration) throws Exception;

    /**
     * 获取条件的类型。
     *
     * @return 条件类型
     */
    public ConditionType getType() {
        return type;
    }
}
