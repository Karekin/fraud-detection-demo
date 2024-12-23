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

import org.apache.flink.cep.pattern.conditions.RichCompositeIterativeCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * 用于将 {@link RichCompositeIterativeCondition} 序列化和反序列化为 JSON 格式的工具类。
 *
 * <p>该类继承自 {@link ClassConditionSpec}，表示复合条件的规范，
 * 支持包含多个嵌套条件（nestedConditions）的逻辑组合操作。
 */
public class RichCompositeConditionSpec extends ClassConditionSpec {

    /**
     * 嵌套条件的列表。
     *
     * <p>每个嵌套条件由 {@link ConditionSpec} 表示，用于定义复合条件的组成部分。
     */
    private final List<ConditionSpec> nestedConditions;

    /**
     * 构造方法。
     *
     * <p>通过 JSON 属性初始化复合条件的类名和嵌套条件列表。
     *
     * @param className 复合条件的类名
     * @param nestedConditions 嵌套条件的列表
     */
    public RichCompositeConditionSpec(
            @JsonProperty("className") String className,
            @JsonProperty("nestedConditions") List<ConditionSpec> nestedConditions) {
        // 调用父类构造方法，初始化类名
        super(className);
        // 将嵌套条件列表存储为新列表，防止外部修改
        this.nestedConditions = new ArrayList<>(nestedConditions);
    }

    /**
     * 获取嵌套条件列表。
     *
     * @return 嵌套条件的列表
     */
    public List<ConditionSpec> getNestedConditions() {
        return nestedConditions;
    }
}

