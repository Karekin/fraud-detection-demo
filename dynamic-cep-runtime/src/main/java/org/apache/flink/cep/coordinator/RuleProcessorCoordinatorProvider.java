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

package org.apache.flink.cep.coordinator;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;

/**
 * {@link RuleProcessorCoordinator} 的提供者类。
 *
 * <p>该类继承自 {@link RecreateOnResetOperatorCoordinator.Provider}，
 * 负责创建和提供 {@link RuleProcessorCoordinator} 的实例。
 *
 * <p>在 Flink 中，协调器（Coordinator）是用于管理和调度操作符的核心组件，
 * 而此提供者类用于初始化协调器及其运行环境。
 */
public class RuleProcessorCoordinatorProvider
        extends RecreateOnResetOperatorCoordinator.Provider {

    private static final long serialVersionUID = 1L; // 序列化版本号，用于确保类的兼容性

    // 操作符的名称
    private final String operatorName;

    // 用于存储规则事件的队列标识符
    private final String ruleQueueId;

    /**
     * 构造 {@link RuleProcessorCoordinatorProvider} 的实例。
     *
     * @param operatorName 操作符的名称，用于标识协调器对应的操作符。
     * @param operatorID   操作符的唯一标识符，与 Flink 的作业调度相关。
     * @param ruleQueueId  用于存储规则事件的队列标识符。
     */
    public RuleProcessorCoordinatorProvider(
            String operatorName,
            OperatorID operatorID,
            String ruleQueueId) {
        super(operatorID); // 调用父类的构造方法，设置操作符的唯一标识符
        this.operatorName = operatorName; // 初始化操作符名称
        this.ruleQueueId = ruleQueueId;   // 初始化规则队列标识符
    }

    /**
     * 创建并返回 {@link RuleProcessorCoordinator} 的实例。
     *
     * <p>此方法通过提供协调器上下文和运行线程的工厂，初始化协调器的运行环境，
     * 并创建一个新的 {@link RuleProcessorCoordinator} 实例。
     *
     * @param context {@link OperatorCoordinator.Context} 提供 Flink 框架与协调器的交互上下文。
     * @return 一个新的 {@link RuleProcessorCoordinator} 实例。
     */
    @Override
    public OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
        // 创建协调器线程名称，用于调试和日志记录
        final String coordinatorThreadName = "RuleProcessorCoordinator-" + operatorName;

        // 创建协调器线程工厂，确保协调器任务在线程安全的环境中运行
        CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new CoordinatorExecutorThreadFactory(
                        coordinatorThreadName, // 协调器线程名称
                        context.getUserCodeClassloader()); // 用户代码类加载器

        // 初始化协调器上下文，用于协调器与 Flink 框架的交互
        CoordinatorContext coordinatorContext =
                new CoordinatorContext(coordinatorThreadFactory, context);

        // 创建并返回 RuleProcessorCoordinator 实例
        return new RuleProcessorCoordinator(
                operatorName, // 操作符名称
                ruleQueueId,  // 规则队列标识符
                coordinatorContext); // 协调器上下文
    }
}

