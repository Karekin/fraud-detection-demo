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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KTD, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.cep.discover;

import org.apache.flink.cep.event.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * 基于定时轮询机制的规则发现器实现类，负责周期性地检测规则更新
 * 继承自RuleDiscoverer接口，提供规则动态发现能力
 */
public abstract class PeriodicRuleDiscoverer
        implements RuleDiscoverer {

    // 日志记录器
    private static final Logger LOG = LoggerFactory.getLogger(PeriodicRuleDiscoverer.class);

    // 规则检查间隔时间（毫秒）
    private final Long intervalMillis;

    // 定时任务调度器
    private final Timer timer;

    // 当前持有的规则列表
    private List<Rule> rules;

    /**
     * 构造方法，初始化规则发现器
     * @param intervalMillis 规则检查间隔时间（毫秒）
     *                       建议值 >= 1000ms，避免高频请求
     */
    public PeriodicRuleDiscoverer(final Long intervalMillis) {
        // 参数校验（示例，实际应添加）
        if (intervalMillis <= 0) {
            throw new IllegalArgumentException("间隔时间必须大于0");
        }
        this.intervalMillis = intervalMillis;
        this.timer = new Timer("Rule-Discoverer-Timer", true); // 使用守护线程
    }

    /**
     * 抽象方法 - 获取最新规则列表
     * 需子类实现具体规则获取逻辑（如：数据库查询/文件读取/API调用等）
     * @return 最新规则列表，不应返回null（推荐返回空列表）
     * @throws Exception 获取规则时可能抛出的异常（如网络异常、解析错误等）
     */
    public abstract List<Rule> getLatestRules() throws Exception;

    /**
     * 启动规则发现流程
     * @param ruleManager 规则管理器，用于接收规则更新通知
     */
    @Override
    public void discoverRuleUpdates(RuleManager ruleManager) {
        // 定时任务参数说明：
        // 0 表示立即执行第一次检查
        // intervalMillis 表示后续每次执行的间隔
        timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            // 1. 获取最新规则
                            List<Rule> latestRules = getLatestRules();

                            // 2. 检查规则是否更新
                            if (isUpdated(latestRules)) {
                                // 3. 更新本地缓存
                                rules = new ArrayList<>(latestRules); // 防御性拷贝

                                LOG.info("检测到规则更新，数量：{}", latestRules.size());

                                // 4. 通知规则管理器
                                ruleManager.onRuleUpdated(Collections.unmodifiableList(rules));
                            }
                        } catch (Exception e) {
                            // 异常处理策略：
                            // 记录错误日志但不中断定时任务，防止单次失败影响后续检查
                            LOG.error("规则获取失败，将在{}ms后重试。错误详情：", intervalMillis, e);
                        }
                    }
                },
                0,          // 首次执行延迟（立即执行）
                intervalMillis // 执行间隔
        );
    }

    /**
     * 资源释放方法，停止规则发现
     * @throws IOException 关闭时可能抛出的IO异常
     */
    @Override
    public void close() throws IOException {
        // 停止定时任务（重要！防止线程泄漏）
        timer.cancel();
        LOG.debug("规则发现器已关闭");
    }

    /**
     * 规则更新判断逻辑
     * @param latestRules 最新获取的规则列表
     * @return true表示规则有更新，需要触发通知
     *
     * 优化点建议：
     * - 可改为基于版本号或时间戳的判断
     * - 当前实现基于集合内容对比，适用于小规模规则集
     */
    public boolean isUpdated(List<Rule> latestRules) {
        // 空值保护
        if (latestRules == null) return false;

        // 三重判断逻辑：
        // 1. 首次初始化（rules为空）
        // 2. 规则数量变化
        // 3. 规则内容变化（基于HashSet的快速判断）
        return rules == null
                || rules.size() != latestRules.size()
                || !new HashSet<>(rules).containsAll(latestRules);
    }
}
