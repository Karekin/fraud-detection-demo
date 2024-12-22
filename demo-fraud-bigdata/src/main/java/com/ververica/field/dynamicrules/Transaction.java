package com.ververica.field.dynamicrules;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 事务类（Transaction），用于表示一个支付交易信息。
 * 包含了交易的相关字段，如交易ID、事件时间、支付方和受益方的ID、支付金额和支付方式等。
 * 此类实现了`TimestampAssignable<Long>`接口，用于为事务指定摄取时间戳。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction implements TimestampAssignable<Long> {

    // 交易ID
    public long transactionId;

    // 事件时间（Unix时间戳，毫秒）
    public long eventTime;

    // 支付方ID
    public long payeeId;

    // 受益方ID
    public long beneficiaryId;

    // 支付金额
    public BigDecimal paymentAmount;

    // 支付类型（现金或信用卡）
    public PaymentType paymentType;

    // 摄取时间戳（数据摄取的时间，单位：毫秒）
    private Long ingestionTimestamp;

    // 时间格式化器，用于解析事件时间的字符串格式
    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withLocale(Locale.US)  // 设置日期时间格式为美国英语
                    .withZone(ZoneOffset.UTC);  // 设置时区为UTC

    /**
     * 支付类型枚举（支付方式），包括现金（CSH）和信用卡（CRD）
     */
    public enum PaymentType {
        CSH("CSH"),  // 现金支付
        CRD("CRD");  // 信用卡支付

        // 支付类型的字符串表示
        String representation;

        // 构造函数
        PaymentType(String repr) {
            this.representation = repr;
        }

        /**
         * 根据字符串表示返回对应的支付类型。
         *
         * @param representation 支付类型的字符串表示
         * @return 返回对应的PaymentType枚举值
         */
        public static PaymentType fromString(String representation) {
            // 遍历所有支付类型，匹配字符串并返回对应枚举
            for (PaymentType b : PaymentType.values()) {
                if (b.representation.equals(representation)) {
                    return b;
                }
            }
            return null;  // 如果没有匹配的支付类型，则返回null
        }
    }

    /**
     * 从CSV格式的字符串创建Transaction对象。
     * CSV格式的字符串应该包含7个字段：交易ID、事件时间、支付方ID、受益方ID、支付方式、支付金额、摄取时间戳。
     *
     * @param line CSV格式的事务数据行
     * @return 返回一个Transaction对象
     */
    public static Transaction fromString(String line) {
        // 按照逗号分隔符拆分字符串，得到字段列表
        List<String> tokens = Arrays.asList(line.split(","));
        int numArgs = 7;  // 期望的字段数量
        // 校验字段数是否正确
        if (tokens.size() != numArgs) {
            throw new RuntimeException(
                    "无效的事务数据： " + line + ". 必需的字段数量： " + numArgs + "，实际字段数：" + tokens.size());
        }

        // 创建一个Transaction对象
        Transaction transaction = new Transaction();

        try {
            // 使用迭代器遍历所有字段并赋值给Transaction对象的各个属性
            Iterator<String> iter = tokens.iterator();
            transaction.transactionId = Long.parseLong(iter.next());  // 交易ID
            // 事件时间的字符串转换为ZonedDateTime对象，再转为毫秒时间戳
            transaction.eventTime = ZonedDateTime.parse(iter.next(), timeFormatter).toInstant().toEpochMilli();
            transaction.payeeId = Long.parseLong(iter.next());  // 支付方ID
            transaction.beneficiaryId = Long.parseLong(iter.next());  // 受益方ID
            // 支付类型通过字符串解析为PaymentType枚举
            transaction.paymentType = PaymentType.fromString(iter.next());
            transaction.paymentAmount = new BigDecimal(iter.next());  // 支付金额
            transaction.ingestionTimestamp = Long.parseLong(iter.next());  // 摄取时间戳
        } catch (NumberFormatException nfe) {
            // 如果字段无法转换为数字，抛出异常
            throw new RuntimeException("无效的记录数据： " + line, nfe);
        }

        return transaction;  // 返回解析后的Transaction对象
    }

    /**
     * 实现TimestampAssignable接口的方法，为Transaction对象分配摄取时间戳。
     *
     * @param timestamp 要分配的摄取时间戳（毫秒）
     */
    @Override
    public void assignIngestionTimestamp(Long timestamp) {
        this.ingestionTimestamp = timestamp;  // 设置摄取时间戳
    }
}
