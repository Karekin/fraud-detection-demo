package com.ververica.field.dynamicrules;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// 使用Lombok注解自动生成构造函数、getter、setter等方法
@Data // 自动生成getter、setter、toString、equals、hashCode方法
@NoArgsConstructor // 自动生成无参构造函数
@AllArgsConstructor // 自动生成全参构造函数
public class Keyed<IN, KEY, ID> {

    // 被包装的输入对象
    private IN wrapped;

    // 用于标识的键值
    private KEY key;

    // 唯一标识符
    private ID id;
}
