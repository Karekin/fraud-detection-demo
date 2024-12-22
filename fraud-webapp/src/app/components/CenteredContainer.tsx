/*
    适用场景：
    这是一个通用的容器组件，适合需要在卡片中垂直居中或动态调整布局的场景，常用于警报、规则或交易等界面中的子组件布局管理。
 */

import React, {forwardRef, ReactNode, CSSProperties} from "react";
import {Card} from "reactstrap"; // 引入 Reactstrap 的 Card 组件，用于作为容器。
import cx from "classnames"; // 引入 classnames 库，用于动态组合 CSS 类名。

/**
 * CenteredContainer 组件
 *
 * 这是一个基于 `Card` 的容器组件，支持动态设置样式和布局，适用于需要垂直居中显示或动态调整间距的场景。
 * 使用 `forwardRef` 允许父组件访问 `Card` DOM 节点的引用。
 */
export const CenteredContainer = forwardRef((props: Props, ref) => {
    return (
        <Card
            innerRef={ref} // 将传入的 ref 绑定到 Card 组件的 DOM 节点上
            className={cx([
                "w-100 overflow-hidden flex-shrink-0", // 固定类：宽度 100%，隐藏溢出内容，防止压缩布局
                props.tooManyItems ? "my-3" : "my-auto", // 动态类：根据警报数量决定是添加边距还是垂直居中
                props.className, // 自定义类名，可由父组件通过 props 传入
            ])}
            style={props.style} // 自定义样式，可由父组件通过 props 传入
        >
            {props.children} {/* 渲染子组件内容 */}
        </Card>
    );
});

/**
 * CenteredContainer 组件的 Props 类型定义。
 */
interface Props {
    tooManyItems: boolean; // 标记是否有过多的项目。如果为 true，组件将添加额外的边距。
    children: ReactNode; // 子节点，容器中的内容。
    className?: string; // 可选的自定义 CSS 类名，用于动态扩展样式。
    style?: CSSProperties; // 可选的内联样式对象，用于动态调整组件的样式。
}
