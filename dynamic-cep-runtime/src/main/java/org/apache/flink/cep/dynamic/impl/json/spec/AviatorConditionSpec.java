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

import org.apache.flink.cep.configuration.ObjectConfiguration;
import org.apache.flink.cep.dynamic.condition.AviatorCondition;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 用于将 {@link AviatorCondition} 序列化和反序列化为 JSON 格式的工具类。
 *
 * <p>该类继承自 {@link ConditionSpec}，表示基于 Aviator 表达式的条件规范，
 * 支持将条件表达式转换为可迭代的条件（IterativeCondition）。
 */
public class AviatorConditionSpec extends ConditionSpec {

    /**
     * 条件的过滤表达式。
     */
    private final String expression;

    /**
     * 构造方法。
     *
     * <p>通过 JSON 属性初始化 Aviator 条件的表达式，并指定条件类型为 {@code AVIATOR}。
     *
     * @param expression Aviator 条件的过滤表达式
     */
    public AviatorConditionSpec(@JsonProperty("expression") String expression) {
        super(ConditionType.AVIATOR); // 指定条件类型为 AVIATOR
        this.expression = expression; // 初始化条件表达式
    }

    /**
     * 获取条件的表达式。
     *
     * @return 条件表达式字符串
     */
    public String getExpression() {
        return expression;
    }

    /**
     * 将条件规范转换为可迭代条件（IterativeCondition）。
     *
     * <p>通过反射加载 {@link AviatorCondition} 类，动态创建条件实例。
     *
     * @param classLoader           用于加载条件类的类加载器
     * @param globalConfiguration   全局配置对象，用于传递参数
     * @return 转换后的 {@link IterativeCondition} 实例
     * @throws Exception 如果加载类或创建实例失败
     */
    @Override
    public IterativeCondition<?> toIterativeCondition(
            ClassLoader classLoader,
            Configuration globalConfiguration) throws Exception {
        return (IterativeCondition<?>)
                classLoader
                        // 加载 AviatorCondition 类
                        .loadClass(AviatorCondition.class.getCanonicalName())
                        // 获取 AviatorCondition 的构造方法
                        .getConstructor(String.class, ObjectConfiguration.class)
                        // 创建 AviatorCondition 实例
                        .newInstance(
                                expression,
                                (ObjectConfiguration) globalConfiguration);
    }
}

