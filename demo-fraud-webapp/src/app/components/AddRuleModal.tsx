/*
    模态框的创建和管理：使用 Reactstrap 的 Modal 组件，封装了规则的添加界面。
    表单处理：处理用户提交的规则数据，支持实时验证和错误反馈。
    示例规则快速添加：提供按钮快速生成示例规则，便于用户测试。
    组件属性和回调：通过 Props 接口定义父组件传递的参数和回调函数，确保类型安全。
 */

import {
    faArrowUp,
    faCalculator,
    faClock,
    faFont,
    faInfoCircle,
    faLaptopCode,
    faLayerGroup,
} from "@fortawesome/free-solid-svg-icons"; // 导入 FontAwesome 图标，用于在字段组中显示相关图标。
import Axios from "axios"; // 导入 Axios，用于进行 HTTP 请求。
import getFormData from "get-form-data"; // 导入 getFormData，用于从表单中提取数据。
import { isArray, pick } from "lodash/fp"; // 导入 lodash/fp 的工具函数，用于数据处理。
import React, { createRef, FC, FormEvent, useState, MouseEvent } from "react"; // 导入 React 和相关的类型。
import CreatableSelect from "react-select/creatable"; // 导入 CreatableSelect，用于支持用户输入和选择的多选框。
import { Alert, Button, Input, Modal, ModalBody, ModalFooter, ModalHeader } from "reactstrap"; // 导入 Reactstrap 组件，用于创建模态框等 UI 元素。
import { Rule, RulePayload } from "../interfaces/"; // 导入 Rule 和 RulePayload 接口，表示规则数据的结构。
import { FieldGroup } from "./FieldGroup"; // 导入自定义组件 FieldGroup，用于将表单字段和图标组合。


// 定义 HTTP 请求头，指定发送 JSON 数据
const headers = { "Content-Type": "application/json" };

// 从 RulePayload 中挑选需要的字段
const pickFields = pick([
    "aggregateFieldName",
    "aggregatorFunctionType",
    "groupingKeyNames",
    "limit",
    "limitOperatorType",
    "ruleState",
    "windowMinutes",
]);

// 定义响应错误的类型，包含错误代码和消息
type ResponseError = {
    error: string; // 错误代码
    message: string; // 错误信息
} | null;

// 示例规则，用于快速填充表单测试数据
const sampleRules: {
    [n: number]: RulePayload;
} = {
    1: {
        aggregateFieldName: "paymentAmount",
        aggregatorFunctionType: "SUM",
        groupingKeyNames: ["payeeId", "beneficiaryId"],
        limit: 20000000,
        limitOperatorType: "GREATER",
        windowMinutes: 43200,
        // tslint:disable-next-line:object-literal-sort-keys
        ruleState: "ACTIVE",
    },
    2: {
        aggregateFieldName: "paymentAmount",
        aggregatorFunctionType: "SUM",
        groupingKeyNames: ["beneficiaryId"],
        limit: 10000000,
        limitOperatorType: "GREATER_EQUAL",
        windowMinutes: 1440,
        // tslint:disable-next-line:object-literal-sort-keys
        ruleState: "ACTIVE",
    },
    3: {
        aggregateFieldName: "COUNT_WITH_RESET_FLINK",
        aggregatorFunctionType: "SUM",
        groupingKeyNames: ["paymentType"],
        limit: 100,
        limitOperatorType: "GREATER_EQUAL",
        windowMinutes: 1440,
        // tslint:disable-next-line:object-literal-sort-keys
        ruleState: "ACTIVE",
    },
};

// 关键词和聚合关键词列表，用于下拉选择字段值
const keywords = ["beneficiaryId", "payeeId", "paymentAmount", "paymentType"];
const aggregateKeywords = ["paymentAmount", "COUNT_FLINK", "COUNT_WITH_RESET_FLINK"];

// 自定义 React memoized 组件，用于提升性能
const MySelect = React.memo(CreatableSelect);

/**
 * 添加规则模态框组件。
 * 提供 UI 和功能以添加新规则或基于示例规则快速创建规则。
 */
export const AddRuleModal: FC<Props> = props => {
    const [error, setError] = useState<ResponseError>(null); // 错误状态管理

    // 处理模态框关闭事件，重置错误并调用父组件的关闭回调
    const handleClosed = () => {
        setError(null);
        props.onClosed();
    };

    // 提交表单数据，调用后端 API 添加规则
    const handleSubmit = (e: FormEvent) => {
        e.preventDefault(); // 防止表单的默认提交行为
        const data = pickFields(getFormData(e.target)) as RulePayload; // 提取表单数据并挑选需要的字段
        data.groupingKeyNames = isArray(data.groupingKeyNames) ? data.groupingKeyNames : [data.groupingKeyNames]; // 确保分组键名是数组

        const rulePayload = JSON.stringify(data); // 将规则数据序列化为 JSON
        const body = JSON.stringify({ rulePayload }); // 构造请求体

        setError(null); // 重置错误状态
        Axios.post<Rule>("/api/rules", body, { headers }) // 发送 POST 请求添加规则
            .then(response => props.setRules(rules => [...rules, { ...response.data, ref: createRef<HTMLDivElement>() }])) // 更新规则列表
            .then(props.onClosed) // 关闭模态框
            .catch(setError); // 捕获错误并更新错误状态
    };

    // 基于示例规则快速添加规则
    const postSampleRule = (ruleId: number) => (e: MouseEvent) => {
        const rulePayload = JSON.stringify(sampleRules[ruleId]); // 获取示例规则
        const body = JSON.stringify({ rulePayload }); // 构造请求体

        Axios.post<Rule>("/api/rules", body, { headers }) // 发送 POST 请求添加规则
            .then(response => props.setRules(rules => [...rules, { ...response.data, ref: createRef<HTMLDivElement>() }])) // 更新规则列表
            .then(props.onClosed) // 关闭模态框
            .catch(setError); // 捕获错误并更新错误状态
    };

    return (
        <Modal
            isOpen={props.isOpen} // 控制模态框的打开状态
            onClosed={handleClosed} // 模态框关闭时的回调
            toggle={props.toggle} // 切换模态框打开状态的回调
            backdropTransition={{ timeout: 75 }} // 设置背景动画过渡时间
            modalTransition={{ timeout: 150 }} // 设置模态框动画过渡时间
            size="lg" // 设置模态框大小
        >
            <form onSubmit={handleSubmit}>
                <ModalHeader toggle={props.toggle}>Add a new Rule</ModalHeader>
                <ModalBody>
                    {/* 显示错误信息 */}
                    {error && <Alert color="danger">{error.error + ": " + error.message}</Alert>}

                    {/* 表单字段组 */}
                    <FieldGroup label="ruleState" icon={faInfoCircle}>
                        <Input type="select" name="ruleState" bsSize="sm">
                            <option value="ACTIVE">ACTIVE</option>
                            <option value="PAUSE">PAUSE</option>
                            <option value="DELETE">DELETE</option>
                        </Input>
                    </FieldGroup>

                    <FieldGroup label="aggregatorFunctionType" icon={faCalculator}>
                        <Input type="select" name="aggregatorFunctionType" bsSize="sm">
                            <option value="SUM">SUM</option>
                            <option value="AVG">AVG</option>
                            <option value="MIN">MIN</option>
                            <option value="MAX">MAX</option>
                        </Input>
                    </FieldGroup>

                    <FieldGroup label="aggregateFieldName" icon={faFont}>
                        <Input name="aggregateFieldName" type="select" bsSize="sm">
                            {aggregateKeywords.map(k => (
                                <option key={k} value={k}>
                                    {k}
                                </option>
                            ))}
                        </Input>
                    </FieldGroup>

                    <FieldGroup label="groupingKeyNames" icon={faLayerGroup}>
                        <MySelect
                            isMulti={true} // 支持多选
                            name="groupingKeyNames" // 字段名
                            className="react-select"
                            classNamePrefix="react-select"
                            options={keywords.map(k => ({ value: k, label: k }))} // 配置选项
                        />
                    </FieldGroup>

                    <FieldGroup label="limitOperatorType" icon={faLaptopCode}>
                        <Input type="select" name="limitOperatorType" bsSize="sm">
                            <option value="EQUAL">EQUAL (=)</option>
                            <option value="NOT_EQUAL">NOT_EQUAL (!=)</option>
                            <option value="GREATER_EQUAL">GREATER_EQUAL (>=)</option>
                            <option value="LESS_EQUAL">LESS_EQUAL ({"<="})</option>
                            <option value="GREATER">GREATER (>)</option>
                            <option value="LESS">LESS ({"<"})</option>
                        </Input>
                    </FieldGroup>

                    <FieldGroup label="limit" icon={faArrowUp}>
                        <Input name="limit" bsSize="sm" type="number" /> {/* 输入阈值 */}
                    </FieldGroup>
                    <FieldGroup label="windowMinutes" icon={faClock}>
                        <Input name="windowMinutes" bsSize="sm" type="number" /> {/* 输入时间窗口 */}
                    </FieldGroup>
                </ModalBody>
                <ModalFooter className="justify-content-between">
                    <div>
                        {/* 添加示例规则按钮 */}
                        <Button color="secondary" onClick={postSampleRule(1)} size="sm" className="mr-2">
                            Sample Rule 1
                        </Button>
                        <Button color="secondary" onClick={postSampleRule(2)} size="sm" className="mr-2">
                            Sample Rule 2
                        </Button>
                        <Button color="secondary" onClick={postSampleRule(3)} size="sm" className="mr-2">
                            Sample Rule 3
                        </Button>
                    </div>
                    <div>
                        {/* 取消和提交按钮 */}
                        <Button color="secondary" onClick={handleClosed} size="sm" className="mr-2">
                            Cancel
                        </Button>
                        <Button type="submit" color="primary" size="sm">
                            Submit
                        </Button>
                    </div>
                </ModalFooter>
            </form>
        </Modal>
    );
};

// 定义组件的Props类型
interface Props {
    toggle: () => void; // 切换模态框状态的方法
    isOpen: boolean; // 模态框是否打开
    onClosed: () => void; // 模态框关闭时的回调方法
    setRules: (fn: (rules: Rule[]) => Rule[]) => void; // 更新规则列表的方法
}
