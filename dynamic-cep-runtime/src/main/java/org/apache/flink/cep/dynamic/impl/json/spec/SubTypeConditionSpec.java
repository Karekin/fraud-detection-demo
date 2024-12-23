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
import org.apache.flink.cep.pattern.conditions.SubtypeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 用于将 {@link SubtypeCondition} 序列化和反序列化为 JSON 格式的工具类。
 *
 * <p>该类继承自 {@link ClassConditionSpec}，支持处理子类型条件（SubtypeCondition）的
 * 特定逻辑，包括子类名称的动态加载和条件实例化。
 */
public class SubTypeConditionSpec extends ClassConditionSpec {

    /**
     * 子类的完全限定名（Fully Qualified Name）。
     *
     * <p>表示子类型条件中指定的子类，用于动态加载和匹配。
     */
    private final String subClassName;

    /**
     * 参数化构造方法。
     *
     * <p>通过 JSON 属性初始化子类型条件的类名和子类名。
     *
     * @param className 父类的完全限定名
     * @param subClassName 子类的完全限定名
     */
    public SubTypeConditionSpec(
            @JsonProperty("className") String className,
            @JsonProperty("subClassName") String subClassName) {
        super(className); // 调用父类构造方法初始化父类名称
        this.subClassName = subClassName; // 初始化子类名称
    }

    /**
     * 从 {@link IterativeCondition} 实例构造子类型条件规范。
     *
     * <p>通过反射提取 {@link SubtypeCondition} 中的子类字段，并将其转换为子类名称。
     *
     * @param condition 子类型条件的实例
     */
    public SubTypeConditionSpec(IterativeCondition condition) {
        super(condition); // 调用父类构造方法初始化父类名称
        try {
            // 使用反射获取 SubtypeCondition 的子类字段值
            subClassName = ((Class<?>) SubtypeCondition.class.getField("subtype").get(condition))
                    .getCanonicalName();
        } catch (Exception e) {
            // 如果反射操作失败，抛出运行时异常
            throw new RuntimeException(e);
        }
    }

    /**
     * 将条件规范转换为 {@link SubtypeCondition} 实例。
     *
     * <p>通过类加载器动态加载子类，并实例化子类型条件。
     *
     * @param classLoader 用于加载类的类加载器
     * @param globalConfiguration 全局配置（未使用）
     * @return 转换后的 {@link SubtypeCondition} 实例
     * @throws Exception 如果加载类或实例化失败
     */
    @Override
    public IterativeCondition<?> toIterativeCondition(
            ClassLoader classLoader, Configuration globalConfiguration) throws Exception {
        // 动态加载子类并实例化 SubtypeCondition
        return new SubtypeCondition<>(classLoader.loadClass(this.getSubClassName()));
    }

    /**
     * 获取子类的完全限定名。
     *
     * @return 子类的完全限定名
     */
    public String getSubClassName() {
        return subClassName;
    }
}

