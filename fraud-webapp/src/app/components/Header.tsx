/*
    AppNavbar：自定义导航栏的样式，移除了默认内边距，并确保在最顶层显示。
    Logo：限制 logo 的最大高度，确保视觉效果一致。
    TransactionsCol：定义交易控制部分的样式，包括边框、布局和对齐。
 */

import logoImage from "app/assets/flink_squirrel_200_color.png"; // 导入应用的 logo 图像。
import React, {FC, useState, Dispatch, SetStateAction} from "react"; // React 的核心库和相关类型。
import {Button, ButtonGroup, Col, Navbar, NavbarBrand} from "reactstrap"; // Reactstrap 的组件，用于构建导航栏和按钮。
import styled from "styled-components/macro"; // 用于定义自定义样式的 styled-components。
import {AddRuleModal} from "./AddRuleModal"; // 导入添加规则的模态框组件。
import {Rule} from "app/interfaces"; // 导入规则的接口定义。

// 自定义导航栏的样式
const AppNavbar = styled(Navbar)`
  && {
    z-index: 1; // 设置层级，确保导航栏在顶部
    justify-content: flex-start; // 左对齐内容
    padding: 0; // 移除内边距
  }
`;

// 自定义 logo 图片的样式
const Logo = styled.img`
  max-height: 40px; // 限制 logo 图片的最大高度
`;

// 自定义交易栏的样式
const TransactionsCol = styled(Col)`
  border-right: 1px solid rgba(255, 255, 255, 0.125); // 设置右边框样式
  display: flex; // 使用 flex 布局
  align-items: center; // 垂直居中对齐
  justify-content: space-between; // 子元素两端对齐
  padding: 0.5em 15px; // 设置内边距
`;

/**
 * 应用程序头部组件。
 * 包含导航栏、交易生成控制按钮、规则操作按钮以及应用 logo。
 */
export const Header: FC<Props> = props => {
    const [modalOpen, setModalOpen] = useState(false); // 管理添加规则模态框的打开状态

    // 打开模态框
    const openRuleModal = () => setModalOpen(true);

    // 关闭模态框
    const closeRuleModal = () => setModalOpen(false);

    // 切换模态框的打开/关闭状态
    const toggleRuleModal = () => setModalOpen(state => !state);

    // 启动交易生成器
    const startTransactions = () => fetch("/api/startTransactionsGeneration").then();

    // 停止交易生成器
    const stopTransactions = () => fetch("/api/stopTransactionsGeneration").then();

    // 同步规则
    const syncRules = () => fetch("/api/syncRules").then();

    // 清除状态
    const clearState = () => fetch("/api/clearState").then();

    // 推送规则到 Flink
    const pushToFlink = () => fetch("/api/rules/pushToFlink").then();

    return (
        <>
            {/* 导航栏 */}
            <AppNavbar color="dark" dark={true}>
                {/* 交易控制部分 */}
                <TransactionsCol xs="2">
                    <NavbarBrand tag="div">Live Transactions</NavbarBrand>
                    <ButtonGroup size="sm">
                        <Button color="success" onClick={startTransactions}>
                            Start
                        </Button>
                        <Button color="danger" onClick={stopTransactions}>
                            Stop
                        </Button>
                    </ButtonGroup>
                </TransactionsCol>

                {/* 规则操作部分 */}
                <Col xs={{size: 5, offset: 1}}>
                    <Button size="sm" color="primary" onClick={openRuleModal}>
                        Add Rule {/* 打开添加规则模态框 */}
                    </Button>

                    <Button size="sm" color="warning" onClick={syncRules}>
                        Sync Rules {/* 同步规则 */}
                    </Button>

                    <Button size="sm" color="warning" onClick={clearState}>
                        Clear State {/* 清除状态 */}
                    </Button>

                    <Button size="sm" color="warning" onClick={pushToFlink}>
                        Push to Flink {/* 推送规则到 Flink */}
                    </Button>
                </Col>

                {/* 应用标题和 logo */}
                <Col xs={{size: 3, offset: 1}} className="justify-content-end d-flex align-items-center">
                    <NavbarBrand tag="div">Apache Flink - Fraud Detection Demo</NavbarBrand>
                    <Logo src={logoImage} title="Apache Flink"/>
                </Col>
            </AppNavbar>

            {/* 添加规则模态框 */}
            <AddRuleModal
                isOpen={modalOpen} // 模态框是否打开
                toggle={toggleRuleModal} // 切换模态框状态的回调
                onClosed={closeRuleModal} // 模态框关闭时的回调
                setRules={props.setRules} // 更新规则列表的回调
            />
        </>
    );
};

/**
 * Header 组件的 Props 类型定义。
 */
interface Props {
    setRules: Dispatch<SetStateAction<Rule[]>>; // 更新规则列表的回调函数，支持类型安全的状态更新。
}
