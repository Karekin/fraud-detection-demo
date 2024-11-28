/*
    核心功能：
    生成交易、规则和警报之间的连接线，并根据规则和警报的状态动态调整线条的颜色和样式。
    提供 handleScroll 回调，用于在滚动时实时更新连接线的位置。

    状态管理：
    使用 useState 保存当前的连接线数组。
    每次规则或警报更新时，通过 useEffect 动态重新计算连接线。

    连接线样式：
    使用 LeaderLine 提供的特性，支持颜色、虚线动画、轮廓和插槽位置的配置。
    红色线条用于高亮触发警报的规则。

    性能优化：
    通过 useCallback 和 useEffect 确保连接线的更新和清理高效且不会造成内存泄漏。

    适用场景：
    用于可视化复杂数据关系，例如展示交易、规则和警报之间的交互。

    清理机制：
    每次组件重新渲染或卸载时，通过 LeaderLine.remove() 移除旧的连接线，防止资源泄漏。
 */

import {Alert, Rule} from "app/interfaces"; // 导入警报和规则的接口定义。
import LeaderLine from "leader-line"; // 导入 LeaderLine 库，用于绘制连接线。
import {flattenDeep} from "lodash/fp"; // 引入 lodash 的 flattenDeep 函数，用于深度扁平化数组。
import {RefObject, useCallback, useEffect, useState} from "react"; // React 核心钩子。

/**
 * useLines 自定义钩子。
 * 用于生成规则、交易和警报之间的动态连接线，并支持实时更新和清理。
 * @param transactionsRef 引用交易组件的 DOM 节点。
 * @param rules 当前的规则列表。
 * @param alerts 当前的警报列表。
 * @returns lines - 当前绘制的所有连接线，handleScroll - 用于更新连接线位置的回调函数。
 */
export const useLines: UseLines = (transactionsRef, rules, alerts) => {
    const [lines, setLines] = useState<Line[]>([]); // 维护当前绘制的所有连接线的状态。

    /**
     * 更新连接线位置的回调函数。
     * 使用 `LeaderLine.position()` 动态调整连接线的位置。
     */
    const updateLines = useCallback(() => {
        lines.forEach(line => {
            try {
                line.line.position(); // 更新每条连接线的位置。
            } catch {
                // 如果更新失败，忽略错误。
            }
        });
    }, [lines]);

    /**
     * useEffect 用于管理连接线的创建和清理。
     * 每次规则或警报更新时，重新计算连接线。
     */
    useEffect(() => {
        const newLines = flattenDeep<Line>(
            rules.map(rule => {
                // 检查规则是否触发了警报。
                const hasAlert = alerts.some(alert => alert.ruleId === rule.id);

                // 创建从交易到规则的输入连接线。
                const inputLine = new LeaderLine(transactionsRef.current, rule.ref.current, {
                    color: hasAlert ? "#dc3545" : undefined, // 如果规则触发了警报，则线条颜色为红色。
                    dash: {animation: true}, // 启用虚线动画。
                    endSocket: "left", // 连接到规则的左侧。
                    startSocket: "right", // 从交易的右侧开始。
                }) as Line;

                // 创建从规则到警报的输出连接线。
                const outputLines = alerts.reduce<Line[]>((acc, alert) => {
                    if (alert.ruleId === rule.id) {
                        return [
                            ...acc,
                            new LeaderLine(rule.ref.current, alert.ref.current, {
                                color: "#fff", // 连接线的颜色为白色。
                                endPlugOutline: true, // 终点带有轮廓。
                                endSocket: "left", // 连接到警报的左侧。
                                outline: true, // 启用线条轮廓。
                                outlineColor: "#dc3545", // 轮廓颜色为红色。
                                startSocket: "right", // 从规则的右侧开始。
                            }) as Line,
                        ];
                    }
                    return acc; // 如果警报与规则无关，则返回原始累积值。
                }, []);

                return [inputLine, ...outputLines]; // 返回输入连接线和输出连接线。
            })
        );

        setLines(newLines); // 更新连接线的状态。

        return () => {
            // 在组件卸载或规则/警报更新时清理所有连接线。
            newLines.forEach(line => line.line.remove());
        };
    }, [transactionsRef, rules, alerts]); // 依赖于交易、规则和警报的更新。

    return {lines, handleScroll: updateLines}; // 返回连接线状态和更新位置的回调。
};

/**
 * UseLines 类型定义。
 * 描述 useLines 钩子的输入参数和返回值类型。
 */
type UseLines = (
    transactionsRef: RefObject<HTMLDivElement>, // 交易组件的 DOM 节点引用。
    rules: Rule[], // 当前的规则列表。
    alerts: Alert[] // 当前的警报列表。
) => {
    lines: Line[]; // 返回的连接线数组。
    handleScroll: () => void; // 用于更新连接线位置的回调函数。
};

/**
 * Line 接口定义。
 * 描述单条连接线的结构和功能。
 */
export interface Line {
    line: {
        color: string; // 连接线的颜色。
        position: () => void; // 更新连接线位置的函数。
        remove: () => void; // 删除连接线的函数。
    };
    ruleId: number; // 连接线所属的规则 ID。
}
