package com.ververica.demo.backend.datasource;

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
 * 交易数据模型类，用于存储和处理交易数据。
 */
@Data   // Lombok注解，自动为所有字段生成getters、setters、toString、equals和hashCode方法
@Builder  // Lombok注解，提供了一种构建对象的方式
@NoArgsConstructor  // Lombok注解，生成无参构造函数
@AllArgsConstructor // Lombok注解，生成全参构造函数
public class Transaction {
    public long transactionId;  // 交易ID
    public long eventTime;      // 交易事件时间（毫秒）
    public long payeeId;        // 付款方ID
    public long beneficiaryId;  // 收款方ID
    public BigDecimal paymentAmount; // 交易金额
    public PaymentType paymentType;  // 交易类型（例如现金或信用卡）

    // 用于格式化时间的格式器，格式为"yyyy-MM-dd HH:mm:ss"，使用UTC时区
    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withLocale(Locale.US)
                    .withZone(ZoneOffset.UTC);

    // 定义支付类型枚举
    public enum PaymentType {
        CSH("CSH"), // 现金支付
        CRD("CRD"); // 信用卡支付

        final String representation;  // 表示形式

        PaymentType(String repr) {
            this.representation = repr;
        }

        /**
         * 根据字符串表示形式解析返回对应的支付类型
         * @param representation 字符串表示形式
         * @return 对应的支付类型，如果找不到返回null
         */
        public static PaymentType fromString(String representation) {
            for (PaymentType b : PaymentType.values()) {
                if (b.representation.equals(representation)) {
                    return b;
                }
            }
            return null;
        }
    }

    /**
     * 从CSV格式的字符串中解析并创建Transaction对象。
     * @param line 代表单个交易数据的CSV字符串
     * @return 解析后创建的Transaction对象
     */
    public static Transaction fromString(String line) {
        List<String> tokens = Arrays.asList(line.split(","));
        int numArgs = 6;
        if (tokens.size() != numArgs) {
            throw new RuntimeException(
                    "Invalid transaction: "
                            + line
                            + ". Required number of arguments: "
                            + numArgs
                            + " found "
                            + tokens.size());
        }

        Transaction transaction = new Transaction();

        try {
            Iterator<String> iter = tokens.iterator();
            transaction.transactionId = Long.parseLong(iter.next());
            transaction.eventTime =
                    ZonedDateTime.parse(iter.next(), timeFormatter).toInstant().toEpochMilli();
            transaction.payeeId = Long.parseLong(iter.next());
            transaction.beneficiaryId = Long.parseLong(iter.next());
            transaction.paymentType = PaymentType.fromString(iter.next());
            transaction.paymentAmount = new BigDecimal(iter.next());
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return transaction;
    }
}
