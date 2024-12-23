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

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 用于描述两个节点（例如 {@link Pattern}）之间事件选择策略的边。
 *
 * <p>该类支持序列化和反序列化为 JSON 格式，用于定义复杂事件处理（CEP）中的节点关系。
 */
public class EdgeSpec {

    /**
     * 边的源节点名称。
     *
     * <p>表示事件选择的起始节点。
     */
    private final String source;

    /**
     * 边的目标节点名称。
     *
     * <p>表示事件选择的目标节点。
     */
    private final String target;

    /**
     * 边的消费策略。
     *
     * <p>定义事件如何从源节点流向目标节点。例如，SKIP_TO_NEXT 或 SKIP_PAST_LAST 等。
     */
    private final ConsumingStrategy type;

    /**
     * 构造方法。
     *
     * <p>通过 JSON 属性初始化边的源节点、目标节点和消费策略。
     *
     * @param source 边的源节点名称
     * @param target 边的目标节点名称
     * @param type 边的消费策略
     */
    public EdgeSpec(
            @JsonProperty("source") String source,
            @JsonProperty("target") String target,
            @JsonProperty("type") ConsumingStrategy type) {
        this.source = source; // 初始化源节点
        this.target = target; // 初始化目标节点
        this.type = type;     // 初始化消费策略
    }

    /**
     * 获取源节点名称。
     *
     * @return 源节点名称
     */
    public String getSource() {
        return source;
    }

    /**
     * 获取目标节点名称。
     *
     * @return 目标节点名称
     */
    public String getTarget() {
        return target;
    }

    /**
     * 获取边的消费策略。
     *
     * @return 消费策略
     */
    public ConsumingStrategy getType() {
        return type;
    }
}

