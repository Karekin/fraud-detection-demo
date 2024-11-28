/*
    样式组件：
    RuleTitle 和 RuleTable 提供自定义的标题和表格样式，确保布局一致且美观。

    逻辑功能：
    删除规则：调用后端 API 并更新状态。
    滚动事件：更新规则和警报的连接线位置，确保连接线跟随元素动态调整。

    规则展示：
    每条规则包括标题、详细信息表格和描述。
    根据规则的状态和警报状态动态调整样式（如边框颜色）。

    Props 定义：
    明确组件依赖的警报、规则和连接线数据，确保类型安全。
 */

import {library} from "@fortawesome/fontawesome-svg-core";
import {
    faArrowUp,
    faCalculator,
    faClock,
    faFont,
    faInfoCircle,
    faLaptopCode,
    faLayerGroup,
    IconDefinition,
} from "@fortawesome/free-solid-svg-icons"; // 导入 FontAwesome 的图标定义。
import {FontAwesomeIcon} from "@fortawesome/react-fontawesome"; // 导入 FontAwesome 的 React 组件。
import Axios from "axios"; // 用于发送 HTTP 请求。
import {isArray} from "lodash/fp"; // 从 lodash/fp 导入工具函数 isArray。
import React, {FC} from "react"; // React 的函数式组件类型。
import {Badge, Button, CardBody, CardFooter, CardHeader, Table} from "reactstrap"; // 从 Reactstrap 导入组件。
import styled from "styled-components/macro"; // 导入 styled-components，用于定义自定义样式。
import {Alert, Rule} from "../interfaces"; // 导入 Alert 和 Rule 接口。
import {CenteredContainer} from "./CenteredContainer"; // 导入自定义容器组件。
import {ScrollingCol} from "./App"; // 导入滚动列布局组件。
import {Line} from "app/utils/useLines"; // 导入 Line 类型，用于表示连接线。

// 将图标添加到 FontAwesome 库
library.add(faInfoCircle);

/**
 * 映射规则状态到对应的徽章颜色。
 */
const badgeColorMap: {
    [s: string]: string;
} = {
    ACTIVE: "success", // 绿色，表示规则激活。
    DELETE: "danger", // 红色，表示规则已删除。
    PAUSE: "warning", // 黄色，表示规则暂停。
};

/**
 * 映射字段名称到对应的 FontAwesome 图标。
 */
const iconMap: {
    [s: string]: IconDefinition;
} = {
    aggregateFieldName: faFont, // 聚合字段名
    aggregatorFunctionType: faCalculator, // 聚合函数类型
    groupingKeyNames: faLayerGroup, // 分组键名
    limit: faArrowUp, // 阈值
    limitOperatorType: faLaptopCode, // 比较操作符类型
    windowMinutes: faClock, // 时间窗口
};

/**
 * 映射操作符到描述符的分隔符。
 */
const seperator: {
    [s: string]: string;
} = {
    EQUAL: "to",
    GREATER: "than",
    GREATER_EQUAL: "than",
    LESS: "than",
    LESS_EQUAL: "than",
    NOT_EQUAL: "to",
};

// 自定义规则标题样式
const RuleTitle = styled.div`
  display: flex;
  align-items: center; // 垂直居中对齐
`;

// 自定义规则表格样式
const RuleTable = styled(Table)`
  && {
    width: calc(100% + 1px); // 调整表格宽度
    border: 0; // 移除边框
    margin: 0;

    td {
      vertical-align: middle !important; // 垂直居中

      &:first-child {
        border-left: 0; // 移除第一列的左边框
      }

      &:last-child {
        border-right: 0; // 移除最后一列的右边框
      }
    }

    tr:first-child {
      td {
        border-top: 0; // 移除第一行的顶部边框
      }
    }
  }
`;

// 定义需要显示的字段列表
const fields = [
    "aggregatorFunctionType",
    "aggregateFieldName",
    "groupingKeyNames",
    "limitOperatorType",
    "limit",
    "windowMinutes",
];

/**
 * 检查规则是否触发了警报。
 * @param alerts 当前的警报列表。
 * @param rule 当前规则。
 * @returns 如果规则触发了警报，则返回 true。
 */
const hasAlert = (alerts: Alert[], rule: Rule) => alerts.some(alert => alert.ruleId === rule.id);

/**
 * 规则展示组件。
 * 用于渲染规则列表，每条规则显示详细信息，并支持删除操作。
 */
export const Rules: FC<Props> = props => {
    /**
     * 删除规则的处理函数。
     * @param id 规则 ID。
     */
    const handleDelete = (id: number) => () => {
        Axios.delete(`/api/rules/${id}`).then(props.clearRule(id)); // 删除规则并更新状态。
    };

    /**
     * 滚动事件处理函数。
     * 用于更新规则和警报的连接线位置。
     */
    const handleScroll = () => {
        props.ruleLines.forEach(line => line.line.position());
        props.alertLines.forEach(line => line.line.position());
    };

    const tooManyRules = props.rules.length > 3; // 如果规则数量超过 3，则标记为“规则过多”。

    return (
        <ScrollingCol xs={{size: 5, offset: 1}} onScroll={handleScroll}>
            {props.rules.map(rule => {
                const payload = JSON.parse(rule.rulePayload); // 解析规则的载荷。

                if (!payload) {
                    return null; // 如果载荷为空，跳过渲染。
                }

                return (
                    <CenteredContainer
                        ref={rule.ref}
                        key={rule.id}
                        tooManyItems={tooManyRules} // 动态调整样式
                        style={{
                            borderColor: hasAlert(props.alerts, rule) ? "#dc3545" : undefined, // 红色边框表示触发警报。
                            borderWidth: hasAlert(props.alerts, rule) ? 2 : 1, // 动态调整边框宽度。
                        }}
                    >
                        {/* 规则标题 */}
                        <CardHeader className="d-flex justify-content-between align-items-center"
                                    style={{padding: "0.3rem"}}>
                            <RuleTitle>
                                <FontAwesomeIcon icon={faInfoCircle} fixedWidth={true} className="mr-2"/>
                                Rule #{rule.id}{" "}
                                <Badge color={badgeColorMap[payload.ruleState]} className="ml-2">
                                    {payload.ruleState}
                                </Badge>
                            </RuleTitle>
                            <Button size="sm" color="danger" outline={true} onClick={handleDelete(rule.id)}>
                                Delete
                            </Button>
                        </CardHeader>
                        {/* 规则详细信息 */}
                        <CardBody className="p-0">
                            <RuleTable size="sm" bordered={true}>
                                <tbody>
                                {fields.map(key => {
                                    const field = payload[key];
                                    return (
                                        <tr key={key}>
                                            <td style={{width: 10}}>
                                                <FontAwesomeIcon icon={iconMap[key]} fixedWidth={true}/>
                                            </td>
                                            <td style={{width: 30}}>{key}</td>
                                            <td>{isArray(field) ? field.map(v => `"${v}"`).join(", ") : field}</td>
                                        </tr>
                                    );
                                })}
                                </tbody>
                            </RuleTable>
                        </CardBody>
                        {/* 规则描述 */}
                        <CardFooter style={{padding: "0.3rem"}}>
                            <em>{payload.aggregatorFunctionType}</em> of <em>{payload.aggregateFieldName}</em> aggregated
                            by "
                            <em>{payload.groupingKeyNames.join(", ")}</em>" is <em>{payload.limitOperatorType}</em>{" "}
                            {seperator[payload.limitOperatorType]} <em>{payload.limit}</em> within an interval of{" "}
                            <em>{payload.windowMinutes}</em> minutes.
                        </CardFooter>
                    </CenteredContainer>
                );
            })}
        </ScrollingCol>
    );
};

/**
 * 组件 Props 类型定义。
 */
interface Props {
    alerts: Alert[]; // 当前警报列表。
    rules: Rule[]; // 当前规则列表。
    clearRule: (id: number) => () => void; // 清除规则的回调函数。
    ruleLines: Line[]; // 规则的连接线列表。
    alertLines: Line[]; // 警报的连接线列表。
}
