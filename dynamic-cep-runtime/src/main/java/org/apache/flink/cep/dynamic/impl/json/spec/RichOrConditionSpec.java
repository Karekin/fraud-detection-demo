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
import org.apache.flink.cep.pattern.conditions.RichOrCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * 用于将 {@link RichOrCondition} 序列化和反序列化为 JSON 格式的工具类。
 *
 * <p>该类继承自 {@link RichCompositeConditionSpec}，表示 OR 条件的规范，
 * 支持对嵌套条件的组合逻辑（OR）的处理。
 */
public class RichOrConditionSpec extends RichCompositeConditionSpec {

    /**
     * 构造方法。
     *
     * <p>通过 JSON 属性初始化 OR 条件的嵌套条件列表。
     *
     * @param nestedConditions OR 条件的嵌套条件列表
     */
    public RichOrConditionSpec(
            @JsonProperty("nestedConditions") List<ConditionSpec> nestedConditions) {
        // 调用父类构造方法，传递类名和嵌套条件
        super(RichOrCondition.class.getCanonicalName(), nestedConditions);
    }

    /**
     * 将条件规范转换为 {@link RichOrCondition} 实例。
     *
     * <p>通过加载嵌套条件的具体实现，并以 OR 的组合逻辑生成条件实例。
     * OR 条件需要至少两个嵌套条件，分别通过索引 0 和索引 1 获取。
     *
     * @param classLoader 用于加载类的类加载器
     * @param globalConfiguration 全局配置，用于传递参数
     * @return 转换后的 {@link RichOrCondition} 实例
     * @throws Exception 如果加载类或创建实例失败
     */
    @Override
    public IterativeCondition<?> toIterativeCondition(
            ClassLoader classLoader, Configuration globalConfiguration) throws Exception {
        // 创建 OR 条件实例，依赖两个嵌套条件
        return new RichOrCondition(
                // 加载并转换第一个嵌套条件
                this.getNestedConditions().get(0).toIterativeCondition(classLoader, globalConfiguration),
                // 加载并转换第二个嵌套条件
                this.getNestedConditions().get(1).toIterativeCondition(classLoader, globalConfiguration));
    }
}

