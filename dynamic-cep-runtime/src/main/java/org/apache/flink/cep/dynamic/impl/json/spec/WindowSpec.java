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
import org.apache.flink.cep.pattern.WithinType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;

/**
 * 用于将 {@link Pattern} 的窗口时间（WindowTime）序列化和反序列化为 JSON 格式的工具类。
 *
 * <p>该类表示窗口的时间约束类型和具体时间值，用于在复杂事件处理（CEP）中定义事件匹配的时间范围。
 */
public class WindowSpec {

    /**
     * 窗口的时间类型。
     *
     * <p>表示时间约束的逻辑类型，例如 "FIRST_AND_LAST" 或 "PREVIOUS_AND_CURRENT"。
     */
    private final WithinType type;

    /**
     * 窗口的时间值。
     *
     * <p>表示实际的时间长度，例如 "5 秒" 或 "10 分钟"。
     */
    private final Time time;

    /**
     * 构造方法。
     *
     * <p>通过 JSON 属性初始化窗口的时间类型和时间值。
     *
     * @param type 窗口的时间类型
     * @param time 窗口的时间值
     */
    public WindowSpec(
            @JsonProperty("type") WithinType type,
            @JsonProperty("time") Time time) {
        this.type = type;
        this.time = time;
    }

    /**
     * 根据窗口时间创建对应的 {@link WindowSpec} 实例。
     *
     * <p>如果窗口包含 "FIRST_AND_LAST" 类型，则使用该类型创建规范对象。
     * 否则，使用 "PREVIOUS_AND_CURRENT" 类型创建规范对象。
     *
     * @param window 窗口时间的映射，键为时间类型，值为时间值
     * @return 对应的 {@link WindowSpec} 实例
     */
    public static WindowSpec fromWindowTime(Map<WithinType, Time> window) {
        if (window.containsKey(WithinType.FIRST_AND_LAST)) {
            // 使用 FIRST_AND_LAST 类型创建
            return new WindowSpec(WithinType.FIRST_AND_LAST, window.get(WithinType.FIRST_AND_LAST));
        } else {
            // 默认使用 PREVIOUS_AND_CURRENT 类型
            return new WindowSpec(
                    WithinType.PREVIOUS_AND_CURRENT, window.get(WithinType.FIRST_AND_LAST));
        }
    }

    /**
     * 获取窗口的时间值。
     *
     * @return 窗口的时间值
     */
    public Time getTime() {
        return time;
    }

    /**
     * 获取窗口的时间类型。
     *
     * @return 窗口的时间类型
     */
    public WithinType getType() {
        return type;
    }
}

