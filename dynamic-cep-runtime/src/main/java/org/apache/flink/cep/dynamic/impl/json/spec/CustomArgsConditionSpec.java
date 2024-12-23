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

import org.apache.flink.cep.dynamic.condition.CustomArgsCondition;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 用于将 {@link CustomArgsCondition} 序列化和反序列化为 JSON 格式的工具类。
 *
 * <p>该类继承自 {@link ConditionSpec}，表示具有自定义参数的条件规范，
 * 支持条件的序列化、反序列化和动态加载。
 */
public class CustomArgsConditionSpec extends ConditionSpec {

    /**
     * 条件的自定义参数数组。
     *
     * <p>这些参数用于定义复杂的过滤逻辑，并由 {@link CustomArgsCondition} 使用。
     */
    private final String[] args;

    /**
     * 条件实现类的完全限定名（Fully Qualified Name）。
     *
     * <p>该字段用于动态加载条件实现类。
     */
    private final String className;

    /**
     * 构造方法。
     *
     * <p>通过 {@link CustomArgsCondition} 实例初始化条件规范对象。
     *
     * @param condition {@link CustomArgsCondition} 实例
     */
    public CustomArgsConditionSpec(CustomArgsCondition<?> condition) {
        super(ConditionType.CLASS); // 指定条件类型为 CLASS
        this.args = condition.getArgs(); // 获取自定义参数
        this.className = condition.getClassName(); // 获取条件类名
    }

    /**
     * 构造方法。
     *
     * <p>通过 JSON 属性初始化条件规范对象。
     *
     * @param args      条件的自定义参数数组
     * @param className 条件实现类的完全限定名
     */
    public CustomArgsConditionSpec(
            @JsonProperty("args") String[] args,
            @JsonProperty("className") String className) {
        super(ConditionType.CLASS); // 指定条件类型为 CLASS
        this.args = args; // 初始化自定义参数
        this.className = className; // 初始化类名
    }

    /**
     * 获取条件的自定义参数数组。
     *
     * @return 自定义参数数组
     */
    public String[] getArgs() {
        return args;
    }

    /**
     * 获取条件实现类的完全限定名。
     *
     * @return 类的完全限定名
     */
    public String getClassName() {
        return className;
    }

    /**
     * 将条件规范转换为 {@link IterativeCondition} 实例。
     *
     * <p>通过反射动态加载 {@link CustomArgsCondition} 实现类，并创建对应的实例。
     *
     * @param classLoader         用于加载条件类的类加载器
     * @param globalConfiguration 全局配置，用于传递参数
     * @return 转换后的 {@link IterativeCondition} 实例
     * @throws Exception 如果加载类或创建实例失败
     */
    @Override
    public IterativeCondition<?> toIterativeCondition(
            ClassLoader classLoader, Configuration globalConfiguration) throws Exception {
        // 使用反射加载类，并创建实例
        return (IterativeCondition<?>)
                classLoader
                        .loadClass(className) // 动态加载条件类
                        .getConstructor(String[].class, String.class) // 获取构造方法
                        .newInstance(args, className); // 创建条件实例
    }
}

