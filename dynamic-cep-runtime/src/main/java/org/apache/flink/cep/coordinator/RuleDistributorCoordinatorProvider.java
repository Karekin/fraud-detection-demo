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


import org.apache.flink.cep.discover.RuleDiscoverer;
import org.apache.flink.cep.discover.RuleDiscovererFactory;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;

/**
 * {@link RuleDistributorCoordinator} 的提供者类。
 *
 * <p>该类继承自 {@link RecreateOnResetOperatorCoordinator.Provider}，用于为规则分发算子提供协调器。
 * 协调器负责管理规则的分发逻辑，发现新规则并动态更新到对应的分发算子中。
 */
public class RuleDistributorCoordinatorProvider
        extends RecreateOnResetOperatorCoordinator.Provider {

    private static final long serialVersionUID = 1L;

    // 算子的名称
    private final String operatorName;

    // 用于发现新规则的工厂
    private final RuleDiscovererFactory discovererFactory;

    // 规则队列的唯一标识符
    private final String ruleQueueId;

    /**
     * 构造函数，初始化 {@link RuleDistributorCoordinatorProvider}。
     *
     * @param operatorName      算子的名称，用于标识该协调器所属的算子
     * @param operatorID        该协调器对应的算子的唯一标识符
     * @param ruleQueueId       规则队列的唯一标识符
     * @param discovererFactory 用于发现新规则的规则发现工厂
     */
    public RuleDistributorCoordinatorProvider(
            String operatorName,
            OperatorID operatorID,
            String ruleQueueId,
            RuleDiscovererFactory discovererFactory) {
        super(operatorID); // 调用父类构造函数，设置算子ID
        this.operatorName = operatorName;
        this.discovererFactory = discovererFactory;
        this.ruleQueueId = ruleQueueId;
    }

    /**
     * 获取算子协调器。
     *
     * <p>该方法根据上下文创建并返回一个 {@link RuleDistributorCoordinator} 实例。
     * 协调器线程由自定义线程工厂创建，确保用户代码运行时类加载器正确。
     *
     * @param context 协调器上下文，包含用户代码类加载器和其他必要信息
     * @return 创建的 {@link RuleDistributorCoordinator} 实例
     */
    @Override
    public OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
        // 定义协调器的线程名称
        final String coordinatorThreadName = "RuleDistributorCoordinator-" + operatorName;

        // 创建协调器线程工厂，用于生成运行协调器的线程
        CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new CoordinatorExecutorThreadFactory(
                        coordinatorThreadName, context.getUserCodeClassloader());

        // 构造协调器上下文，包含线程工厂和上下文信息
        CoordinatorContext coordinatorContext =
                new CoordinatorContext(coordinatorThreadFactory, context);

        // 创建并返回 RuleDistributorCoordinator 实例
        return new RuleDistributorCoordinator(
                operatorName,                  // 算子名称
                ruleQueueId,                   // 规则队列标识
                discovererFactory,             // 规则发现工厂
                coordinatorContext);           // 协调器上下文
    }
}

