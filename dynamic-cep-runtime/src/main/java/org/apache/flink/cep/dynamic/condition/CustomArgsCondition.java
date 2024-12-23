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

package org.apache.flink.cep.dynamic.condition;

import org.apache.flink.annotation.Internal;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

/**
 * 支持自定义参数的条件类。
 *
 * <p>该抽象类继承自 {@link SimpleCondition}，用于定义带有自定义参数的条件逻辑。
 * 自定义参数以 JSON 格式传递，并通过子类实现具体的过滤逻辑。
 *
 * @param <T> 条件应用的事件类型
 */
@Internal
public abstract class CustomArgsCondition<T> extends SimpleCondition<T> {

    private static final long serialVersionUID = 1L; // 序列化版本号，用于类版本兼容

    /**
     * 条件的自定义参数。
     *
     * <p>这些参数以字符串数组形式存储，通常以 JSON 形式传递，用于在子类中定义
     * 更复杂的条件逻辑。
     */
    private final String[] args;

    /**
     * 实现条件逻辑的类名称。
     *
     * <p>该字段用于标识具体实现类，便于在反射或动态加载场景中使用。
     */
    private final String className;

    /**
     * 构造方法。
     *
     * <p>初始化自定义参数和类名称。
     *
     * @param args 自定义参数数组
     * @param className 实现条件逻辑的类名称
     */
    public CustomArgsCondition(String[] args, String className) {
        this.args = args; // 初始化自定义参数
        this.className = className; // 初始化类名称
    }

    /**
     * 获取自定义参数。
     *
     * @return 自定义参数数组
     */
    public String[] getArgs() {
        return args;
    }

    /**
     * 获取实现条件逻辑的类名称。
     *
     * @return 类名称
     */
    public String getClassName() {
        return className;
    }
}
