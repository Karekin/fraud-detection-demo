/*
    FieldGroup 组件：
    用于构建表单中的一个字段组，包含标签（带图标）和对应的输入字段。
    标签列和输入列分别使用自定义样式组件 LabelColumn 和 InputColumn。
    标签中支持 FontAwesome 图标，通过 props.icon 动态渲染。

    适用场景：
    表单字段的布局设计。
    支持带图标的标签，适用于复杂表单或需要更清晰提示的 UI。
 */

import {IconDefinition} from "@fortawesome/free-solid-svg-icons"; // 导入 FontAwesome 图标的类型定义。
import {FontAwesomeIcon} from "@fortawesome/react-fontawesome"; // 导入 FontAwesome 图标组件。
import React, {FC} from "react"; // 从 React 导入函数组件类型 FC。
import {Col, FormGroup, Label} from "reactstrap"; // 从 Reactstrap 导入布局和表单相关组件。
import styled from "styled-components"; // 引入 styled-components，用于定义自定义样式。

/**
 * 自定义标签列的样式。
 * - 右对齐文本。
 * - 使用 `white-space` 和 `text-overflow` 确保内容不会超出容器。
 * - 设置弹性布局参数，优先分配 33% 的空间。
 */
const LabelColumn = styled(Label)`
  text-align: right; // 标签文本右对齐
  white-space: nowrap; // 禁止文本换行
  overflow: hidden; // 超出部分隐藏
  text-overflow: ellipsis; // 超出部分显示省略号
  flex-basis: 33%; // 初始分配 33% 的宽度
  flex: 1 1 auto; // 设置弹性布局参数
`;

/**
 * 自定义输入列的样式。
 * - 设置弹性布局参数，优先分配 67% 的空间。
 */
const InputColumn = styled(Col)`
  flex-basis: 67%; // 初始分配 67% 的宽度
  flex: 1 1 auto; // 设置弹性布局参数
`;

/**
 * FieldGroup 组件
 * 用于构建表单中的单个字段组，包括一个带图标的标签和对应的输入字段。
 */
export const FieldGroup: FC<Props> = props => (
    <FormGroup className="row"> {/* 使用 Reactstrap 的 FormGroup 组件，带有 Bootstrap 的 `row` 样式 */}
        {/* 标签列，显示图标和标签文本 */}
        <LabelColumn className="col-sm-4">
            <FontAwesomeIcon icon={props.icon} fixedWidth={true} className="mr-2"/> {/* 显示图标 */}
            {props.label} {/* 显示标签文本 */}
        </LabelColumn>
        {/* 输入列，渲染子组件 */}
        <InputColumn sm="8">{props.children}</InputColumn>
    </FormGroup>
);

/**
 * Props 接口定义
 * 定义 FieldGroup 组件所需的属性。
 */
interface Props {
    label: string; // 标签文本，显示在输入字段的左侧。
    icon: IconDefinition; // 图标定义，用于显示在标签左侧的 FontAwesome 图标。
}
