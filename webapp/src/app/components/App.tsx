/*
    状态管理：使用 useState 管理规则、警报和连接线的状态。
    生命周期管理：使用 useEffect 加载规则、更新连接线和处理组件的卸载。
    WebSocket 通信：使用 SockJsClient 监听警报和延迟消息，实时更新应用状态。
    动态连接线：使用 LeaderLine 库动态绘制规则与交易、警报之间的连接线，并根据警报状态更新线的样式。
    组件布局：使用 Reactstrap 和 styled-components 实现响应式布局。
 */

import {Header, Alerts, Rules, Transactions} from "app/components"; // 引入应用程序的主要组件。
import {Rule, Alert} from "app/interfaces"; // 引入规则和警报的接口定义。
import Axios from "axios"; // 用于发送 HTTP 请求。
import React, {createRef, FC, useEffect, useRef, useState} from "react"; // React 核心库及其常用钩子。
import {Col, Container, Row} from "reactstrap"; // Reactstrap 的布局组件。
import styled from "styled-components/macro"; // 用于定义组件的样式。
import SockJsClient from "react-stomp"; // SockJS 客户端，用于处理 WebSocket 连接。
import uuid from "uuid/v4"; // 生成唯一 ID 的工具。
import LeaderLine from "leader-line"; // 用于绘制连接线的库。
import {intersectionWith, find} from "lodash/fp"; // Lodash 的函数式编程工具。

import "../assets/app.scss"; // 引入应用的样式。
import {Line} from "app/utils/useLines"; // 引入 Line 类型，用于描述连接线。

// 规则超时时间，单位为毫秒
const RULE_TIMEOUT = 5 * 1000;

// 使用 styled-components 定义主布局容器的样式
const LayoutContainer = styled.div`
  display: flex;
  flex-direction: column;
  width: 100%;
  max-height: 100vh;
  height: 100vh;
  overflow: hidden;
`;

// 定义一个可以滚动的列布局样式
export const ScrollingCol = styled(Col)`
  overflow-y: scroll;
  max-height: 100%;
  display: flex;
  flex-direction: column;
`;

/**
 * 应用程序主组件。
 * 提供规则管理、交易显示和警报展示的综合界面。
 */
export const App: FC = () => {
    const [rules, setRules] = useState<Rule[]>([]); // 存储规则的状态
    const [alerts, setAlerts] = useState<Alert[]>([]); // 存储警报的状态
    const [ruleLines, setRuleLines] = useState<Line[]>([]); // 存储规则的连接线
    const [alertLines, setAlertLines] = useState<Line[]>([]); // 存储警报的连接线

    const transactionsRef = useRef<HTMLDivElement>(null); // 用于引用交易列表的 DOM 节点

    // 获取规则数据，并为每个规则创建引用
    useEffect(() => {
        Axios.get<Rule[]>("/api/rules").then(response =>
            setRules(response.data.map(rule => ({...rule, ref: createRef<HTMLDivElement>()})))
        );
    }, []);

    // 为规则创建连接线
    useEffect(() => {
        const newLines = rules.map(rule => {
            try {
                return {
                    line: new LeaderLine(transactionsRef.current, rule.ref.current, {
                        dash: {animation: true}, // 设置虚线动画
                        endSocket: "left",
                        startSocket: "right",
                    }),
                    ruleId: rule.id,
                };
            } catch (e) {
                return {
                    line: {
                        // tslint:disable-next-line:no-empty
                        position: () => {
                        }, // 空实现，用于防止报错
                        // tslint:disable-next-line:no-empty
                        remove: () => {
                        },
                    },
                    ruleId: rule.id,
                };
            }
        });

        setRuleLines(newLines);

        // 在组件卸载时移除所有连接线
        return () => newLines.forEach(line => line.line.remove());
    }, [rules]);

    // 更新连接线的颜色，标记哪些规则触发了警报
    useEffect(() => {
        const alertingRules = intersectionWith((rule, alert) => rule.id === alert.ruleId, rules, alerts).map(
            rule => rule.id
        );
        ruleLines.forEach(line => {
            try {
                line.line.color = alertingRules.includes(line.ruleId) ? "#dc3545" : "#ff7f50"; // 红色表示触发警报，橙色表示正常状态
            } catch (e) {
                // 忽略错误
            }
        });
    }, [rules, alerts, ruleLines]);

    // 为警报创建连接线
    useEffect(() => {
        const newLines = alerts.map(alert => {
            const rule = find(r => r.id === alert.ruleId, rules);

            return {
                line: new LeaderLine(rule!.ref.current, alert.ref.current, {
                    color: "#fff",
                    endPlugOutline: true,
                    endSocket: "left",
                    outline: true,
                    outlineColor: "#dc3545", // 设置警报连接线的样式
                    startSocket: "right",
                }),
                ruleId: rule!.id,
            };
        });

        setAlertLines(newLines);

        // 在组件卸载时移除所有警报连接线
        return () => newLines.forEach(line => line.line.remove());
    }, [alerts, rules]);

    // 清除指定的规则
    const clearRule = (id: number) => () => setRules(rules.filter(rule => id !== rule.id));

    // 清除指定的警报
    const clearAlert = (id: number) => () => {
        setAlerts(state => {
            const newAlerts = [...state];
            newAlerts.splice(id, 1);
            return newAlerts;
        });
    };

    // 处理新接收到的警报消息
    const handleMessage = (alert: Alert) => {
        const alertId = uuid(); // 为警报生成唯一 ID
        const newAlert = {
            ...alert,
            alertId,
            ref: createRef<HTMLDivElement>(),
            timeout: setTimeout(() => setAlerts(state => state.filter(a => a.alertId !== alertId)), RULE_TIMEOUT), // 自动移除超时的警报
        };

        setAlerts((state: Alert[]) => {
            const filteredState = state.filter(a => a.ruleId !== alert.ruleId); // 确保同一规则的旧警报被替换
            return [...filteredState, newAlert].sort((a, b) => (a.ruleId > b.ruleId ? 1 : -1)); // 按规则 ID 排序
        });
    };

    // 处理延迟消息（目前仅记录日志）
    const handleLatencyMessage = (latency: string) => {
        // tslint:disable-next-line:no-console
        console.info(latency);
    };

    return (
        <>
            {/* SockJS 客户端，用于监听警报和延迟主题的消息 */}
            <SockJsClient url="/ws/backend" topics={["/topic/alerts"]} onMessage={handleMessage}/>
            <SockJsClient url="/ws/backend" topics={["/topic/latency"]} onMessage={handleLatencyMessage}/>

            <LayoutContainer>
                <Header setRules={setRules}/> {/* 渲染头部组件 */}
                <Container fluid={true} className="flex-grow-1 d-flex w-100 flex-column overflow-hidden">
                    <Row className="flex-grow-1 overflow-hidden">
                        {/* 渲染交易、规则和警报组件 */}
                        <Transactions ref={transactionsRef}/>
                        <Rules clearRule={clearRule} rules={rules} alerts={alerts} ruleLines={ruleLines}
                               alertLines={alertLines}/>
                        <Alerts alerts={alerts} clearAlert={clearAlert} lines={alertLines}/>
                    </Row>
                </Container>
            </LayoutContainer>
        </>
    );
};
