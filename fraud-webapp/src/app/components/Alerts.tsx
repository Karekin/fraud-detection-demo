/*
  整体结构：
  使用 Alerts 组件来渲染一个警报列表。
  每个警报包括头部（标题和清除按钮）、主体（警报详细信息）和底部（简要描述）。

  主要功能：
  支持滚动事件，通过 handleScroll 函数更新警报之间的连接线位置。
  提供清除警报的功能，通过 clearAlert 回调函数实现。
  使用 AlertTable 自定义样式表格渲染警报内容。
 */

import React, {FC} from "react";
import {Button, CardBody, CardHeader, Table, CardFooter, Badge} from "reactstrap"; // 引入 Reactstrap 的组件，用于构建 UI。
import styled from "styled-components/macro"; // 引入 styled-components，用于定义样式。
import {faArrowRight} from "@fortawesome/free-solid-svg-icons"; // 引入 FontAwesome 的图标。
import {FontAwesomeIcon} from "@fortawesome/react-fontawesome"; // FontAwesome 图标组件。

import {Alert} from "../interfaces"; // 引入 Alert 接口，表示警报数据结构。
import {CenteredContainer} from "./CenteredContainer"; // 自定义组件，用于居中的容器。
import {ScrollingCol} from "./App"; // 自定义组件，用于滚动的列布局。
import {Payment, Payee, Details, Beneficiary, paymentTypeMap} from "./Transactions"; // 引入交易相关的自定义组件。
import {Line} from "app/utils/useLines"; // 引入 Line 类型，表示连接线。

// 使用 styled-components 定义一个表格的样式
const AlertTable = styled(Table)`
  && {
    width: calc(100% + 1px); // 设置表格宽度
    border: 0; // 移除边框
    margin: 0;

    td {
      vertical-align: middle !important; // 单元格内容垂直居中

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

/**
 * 警报展示组件。
 * 该组件展示一组警报信息，每个警报显示了触发规则的详细信息和相关的交易数据。
 */
export const Alerts: FC<Props> = props => {
    const tooManyAlerts = props.alerts.length > 4; // 如果警报数量大于 4，则标记为“太多警报”。

    /**
     * 滚动事件处理函数，用于更新每条连接线的位置。
     */
    const handleScroll = () => {
        props.lines.forEach(line => line.line.position()); // 调整每条线的位置以匹配警报的滚动。
    };

    return (
        <ScrollingCol xs={{size: 3, offset: 1}} onScroll={handleScroll}>
            {props.alerts.map((alert, idx) => {
                const t = alert.triggeringEvent; // 获取触发警报的交易事件数据。
                return (
                    <CenteredContainer
                        key={idx} // 警报的唯一键
                        className="w-100"
                        ref={alert.ref} // 关联到警报的 React 引用
                        tooManyItems={tooManyAlerts} // 标记是否有太多警报
                        style={{borderColor: "#ffc107", borderWidth: 2}} // 设置边框样式
                    >
                        {/* 卡片头部，显示警报标题和清除按钮 */}
                        <CardHeader>
                            Alert
                            <Button size="sm" color="primary" onClick={props.clearAlert(idx)} className="ml-3">
                                Clear Alert
                            </Button>
                        </CardHeader>

                        {/* 卡片主体，显示警报的详细信息 */}
                        <CardBody className="p-0">
                            <AlertTable size="sm" bordered={true}>
                                <tbody>
                                <tr>
                                    <td>Transaction</td>
                                    <td>{alert.triggeringEvent.transactionId}</td>
                                    {/* 显示交易 ID */}
                                </tr>
                                <tr>
                                    <td colSpan={2} className="p-0" style={{borderBottomWidth: 3}}>
                                        {/* 显示交易详细信息 */}
                                        <Payment className="px-2">
                                            <Payee>{t.payeeId}</Payee> {/* 显示付款人 ID */}
                                            <Details>
                                                <FontAwesomeIcon className="mx-1"
                                                                 icon={paymentTypeMap[t.paymentType]}/> {/* 显示支付类型图标 */}
                                                <Badge
                                                    color="info">${parseFloat(t.paymentAmount.toString()).toFixed(2)}</Badge> {/* 显示支付金额 */}
                                                <FontAwesomeIcon className="mx-1"
                                                                 icon={faArrowRight}/> {/* 显示箭头图标 */}
                                            </Details>
                                            <Beneficiary>{t.beneficiaryId}</Beneficiary> {/* 显示受益人 ID */}
                                        </Payment>
                                    </td>
                                </tr>
                                <tr>
                                    <td>Rule</td>
                                    <td>{alert.ruleId}</td>
                                    {/* 显示规则 ID */}
                                </tr>
                                <tr>
                                    <td>Amount</td>
                                    <td>{alert.triggeringValue}</td>
                                    {/* 显示触发金额 */}
                                </tr>
                                <tr>
                                    <td>Of</td>
                                    <td>{alert.violatedRule.aggregateFieldName}</td>
                                    {/* 显示违反规则的聚合字段名 */}
                                </tr>
                                </tbody>
                            </AlertTable>
                        </CardBody>

                        {/* 卡片底部，显示简要的警报描述 */}
                        <CardFooter style={{padding: "0.3rem"}}>
                            Alert for Rule <em>{alert.ruleId}</em> caused by Transaction{" "}
                            <em>{alert.triggeringEvent.transactionId}</em> with
                            Amount <em>{alert.triggeringValue}</em> of{" "}
                            <em>{alert.violatedRule.aggregateFieldName}</em>.
                        </CardFooter>
                    </CenteredContainer>
                );
            })}
        </ScrollingCol>
    );
};

/**
 * 警报组件的 Props 类型定义。
 */
interface Props {
    alerts: Alert[]; // 警报列表
    clearAlert: any; // 用于清除警报的回调函数
    lines: Line[]; // 连接线的列表，用于在滚动时调整位置
}
