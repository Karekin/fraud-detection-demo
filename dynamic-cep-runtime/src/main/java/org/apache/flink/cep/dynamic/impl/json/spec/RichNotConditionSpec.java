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
import org.apache.flink.cep.pattern.conditions.RichNotCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * 用于将 {@link RichNotCondition} 序列化和反序列化为 JSON 格式的工具类。
 *
 * <p>该类继承自 {@link RichCompositeConditionSpec}，表示 NOT 条件的规范，
 * 支持对单一嵌套条件的逻辑非（NOT）操作。
 */
public class RichNotConditionSpec extends RichCompositeConditionSpec {

    /**
     * 构造方法。
     *
     * <p>通过 JSON 属性初始化 NOT 条件的嵌套条件列表。
     *
     * @param nestedConditions NOT 条件的嵌套条件列表
     */
    public RichNotConditionSpec(
            @JsonProperty("nestedConditions") List<ConditionSpec> nestedConditions) {
        // 调用父类构造方法，传递类名和嵌套条件
        super(RichNotCondition.class.getCanonicalName(), nestedConditions);
    }

    /**
     * 将条件规范转换为 {@link RichNotCondition} 实例。
     *
     * <p>通过加载嵌套条件的具体实现，并将其包裹为 NOT 条件实例。
     * NOT 条件要求嵌套条件列表仅包含一个条件。
     *
     * @param classLoader 用于加载类的类加载器
     * @param globalConfiguration 全局配置，用于传递参数
     * @return 转换后的 {@link RichNotCondition} 实例
     * @throws Exception 如果加载类或创建实例失败
     */
    @Override
    public IterativeCondition<?> toIterativeCondition(
            ClassLoader classLoader, Configuration globalConfiguration) throws Exception {
        // 创建 NOT 条件实例，基于唯一的嵌套条件
        return new RichNotCondition(
                // 加载并转换第一个嵌套条件
                this.getNestedConditions().get(0).toIterativeCondition(classLoader, globalConfiguration));
    }
}

