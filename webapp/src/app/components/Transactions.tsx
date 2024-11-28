/*
    实时交易数据展示：
    通过 WebSocket 实时接收交易数据，并显示最近 33 条记录。

    滑块控制生成器速率：
    提供滑块调整生成器速率，并通过 API 通知后端更新速率。

    高效渲染：
    使用虚拟列表（react-virtualized），即使数据量较大也能保持高效。

    动态样式：
    根据生成速率显示警告信息和动态样式。

    良好的用户体验：
    使用 FontAwesome 图标和动态徽章美化界面，同时保证功能直观易用。
 */

import {FontAwesomeIcon} from "@fortawesome/react-fontawesome"; // 导入 FontAwesome 图标组件。
import React, {forwardRef, useRef, useState} from "react"; // React 核心库及相关钩子。
import {
    faArrowRight,
    faCreditCard,
    faMoneyBill,
    faQuestionCircle,
    IconDefinition,
    faRocket,
} from "@fortawesome/free-solid-svg-icons"; // 导入 FontAwesome 图标。
import {Badge, Card, CardBody, CardHeader, Col} from "reactstrap"; // Reactstrap 的组件。
import styled from "styled-components/macro"; // 用于定义自定义样式。
import {Transaction} from "../interfaces"; // 导入 Transaction 接口。
import Slider from "react-rangeslider"; // 引入滑块组件。
import {useLocalStorage, useUpdateEffect} from "react-use"; // 自定义 React 钩子，用于本地存储和更新。
import {AutoSizer, List, ListRowRenderer} from "react-virtualized"; // 虚拟列表组件，用于高效渲染大量数据。
import SockJsClient from "react-stomp"; // WebSocket 客户端，用于实时接收交易数据。
import "react-virtualized/styles.css"; // 引入虚拟列表的样式。

/**
 * 支付类型与图标的映射表。
 */
export const paymentTypeMap: {
    [s: string]: IconDefinition;
} = {
    CRD: faCreditCard, // 信用卡支付图标
    CSH: faMoneyBill, // 现金支付图标
    undefined: faQuestionCircle, // 未知支付类型图标
};

/**
 * 自定义样式的交易卡片组件。
 * - 100% 宽度和高度，移除左边框。
 */
const TransactionsCard = styled(Card)`
  width: 100%;
  height: 100%;
  border-left: 0 !important;

  .rangeslider__handle {
    &:focus {
      outline: 0;
    }
  }
`;

/**
 * 自定义交易表头样式。
 * - 居中显示，添加底部边框。
 */
const TransactionsHeading = styled.div`
  display: flex;
  justify-content: space-between;
  border-bottom: 1px solid rgba(0, 0, 0, 0.125);
  font-weight: 500;
`;

/**
 * 单个交易条目的样式。
 * - 对齐方式和高度统一。
 * - 支持动态调整样式，如高亮显示。
 */
export const Payment = styled.div`
  position: relative;
  display: flex;
  align-items: center;
  justify-content: space-around;
  height: 40px;
  border-top: 1px solid rgba(0, 0, 0, 0.125);

  &.text-danger {
    border-top: 1px solid #dc3545; // 红色边框表示危险状态
    border-bottom: 1px solid #dc3545;
  }

  &.text-danger + & {
    border-top: 0;
  }

  &:first-of-type {
    border-top: none;
  }
`;

/**
 * 通用的 FlexSpan 样式，用于交易条目的子项。
 */
const FlexSpan = styled.span`
  display: inline-flex;
  align-items: center;
  width: 100px;
  flex-basis: 33%;
  flex: 1 1 auto;
`;

/** 付款方的样式 */
export const Payee = styled(FlexSpan)`
  justify-content: flex-start;
`;

/** 交易详情的样式 */
export const Details = styled(FlexSpan)`
  justify-content: center;
`;

/** 收款方的样式 */
export const Beneficiary = styled(FlexSpan)`
  justify-content: flex-end;
`;

/** 覆盖层样式，当交易生成速率过高时显示警告 */
const TransactionsOverlay = styled.div`
  position: absolute;
  z-index: 10;
  background: rgba(0, 0, 0, 0.7);
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
  text-align: center;
  color: white;
`;

/** 火箭图标样式，用于速率过高的提示 */
const Rocket = styled.span`
  font-size: 500%;
  width: 100%;
`;

/**
 * 模拟值转换函数。
 * 根据生成器速率调整交易速率。
 * @param value 生成器速率的原始值
 * @returns 模拟值
 */
const getFakeValue = (value: number) => {
    return value <= 10 ? value : value <= 20 ? (value - 10) * 10 : (value - 20) * 100;
};

/**
 * 交易组件，用于实时显示交易信息。
 * @returns JSX.Element
 */
export const Transactions = React.memo(
    forwardRef<HTMLDivElement, {}>((props, ref) => {
        const list = useRef<List>(null); // 用于引用虚拟列表组件。
        const [transactions, setTransactions] = useState<Transaction[]>([]); // 交易数据的状态。

        /**
         * 添加新交易。
         * 仅保留最近的 33 条交易。
         */
        const addTransaction = (transaction: Transaction) => setTransactions(state => [...state.slice(-33), transaction]);

        const [generatorSpeed, setGeneratorSpeed] = useLocalStorage("generatorSpeed", 1); // 本地存储生成器速率。

        /**
         * 滑块值改变的处理函数。
         * @param val 新的滑块值
         */
        const handleSliderChange = (val: number) => setGeneratorSpeed(val);

        /**
         * 当生成器速率改变时，发送请求通知后端更新速率。
         */
        useUpdateEffect(() => {
            fetch(`/api/generatorSpeed/${getFakeValue(generatorSpeed)}`);
        }, [generatorSpeed]);

        /**
         * 渲染虚拟列表的单行。
         * @param key 唯一标识
         * @param index 当前行索引
         * @param style 行的样式
         */
        const renderRow: ListRowRenderer = ({key, index, style}) => {
            const t = transactions[index];

            return (
                <Payment key={key} style={style} className="px-2">
                    <Payee>{t.payeeId}</Payee> {/* 显示付款方 ID */}
                    <Details>
                        <FontAwesomeIcon className="mx-1"
                                         icon={paymentTypeMap[t.paymentType]}/> {/* 显示支付类型图标 */}
                        <Badge
                            color="info">${parseFloat(t.paymentAmount.toString()).toFixed(2)}</Badge> {/* 显示金额 */}
                        <FontAwesomeIcon className="mx-1" icon={faArrowRight}/> {/* 显示箭头图标 */}
                    </Details>
                    <Beneficiary>{t.beneficiaryId}</Beneficiary> {/* 显示收款方 ID */}
                </Payment>
            );
        };

        return (
            <>
                {/* 通过 WebSocket 接收实时交易数据 */}
                <SockJsClient url="/ws/backend" topics={["/topic/transactions"]} onMessage={addTransaction}/>
                <Col xs="2" className="d-flex flex-column px-0">
                    <TransactionsCard innerRef={ref}>
                        {/* 生成器速率滑块 */}
                        <CardHeader className="d-flex align-items-center py-0 justify-content-between">
                            <div style={{width: 160}} className="mr-3 d-inline-block">
                                <Slider
                                    value={generatorSpeed}
                                    onChange={handleSliderChange}
                                    max={30}
                                    min={0}
                                    tooltip={false}
                                    step={1}
                                />
                            </div>
                            <span>{getFakeValue(generatorSpeed)}</span>
                        </CardHeader>
                        {/* 交易列表主体 */}
                        <CardBody className="p-0 mb-0" style={{pointerEvents: "none"}}>
                            <TransactionsOverlay hidden={generatorSpeed < 16}>
                                <div>
                                    <Rocket>
                                        <FontAwesomeIcon icon={faRocket}/> {/* 显示火箭图标 */}
                                    </Rocket>
                                    <h2>Transactions per-second too high to render...</h2> {/* 显示警告信息 */}
                                </div>
                            </TransactionsOverlay>
                            <TransactionsHeading className="px-2 py-1">
                                <span>Payer</span>
                                <span>Amount</span>
                                <span>Beneficiary</span>
                            </TransactionsHeading>
                            <AutoSizer>
                                {({height, width}) => (
                                    <List
                                        ref={list}
                                        height={height}
                                        width={width}
                                        rowHeight={40}
                                        rowCount={transactions.length - 1}
                                        rowRenderer={renderRow} // 渲染交易数据的行
                                    />
                                )}
                            </AutoSizer>
                        </CardBody>
                    </TransactionsCard>
                </Col>
            </>
        );
    })
);
