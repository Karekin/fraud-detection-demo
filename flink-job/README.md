# Dynamic Fraud Detection Demo with Apache Flink

## Introduction


### Instructions (local execution with netcat):

1. Start `netcat`:
```
nc -lk 9999
```
2. Run main method of `com.ververica.field.dynamicrules.Main`
3. Submit to netcat in correct format:


Rule Example:
rule_id, (rule_state), (aggregation keys), (unique keys), (aggregateFieldName field), (aggregation function), (limit operator), (limit), (window size in minutes)

##### Examples:

1,(active),(paymentType),,(paymentAmount),(SUM),(>),(50),(20)
1,(delete),(paymentType),,(paymentAmount),(SUM),(>),(50),(20)
2,(active),(payeeId),,(paymentAmount),(SUM),(>),(10),(20)
2,(pause),(payeeId),,(paymentAmount),(SUM),(>),(10),(20)

##### Examples JSON:
{ "ruleId": 1, "ruleState": "ACTIVE", "groupingKeyNames": ["paymentType"], "unique": [], "aggregateFieldName": "paymentAmount", "aggregatorFunctionType": "SUM","limitOperatorType": "GREATER","limit": 500, "windowMinutes": 20}

##### Examples of Control Commands:

{"ruleState": "CONTROL", "controlType":"DELETE_RULES_ALL"}
{"ruleState": "CONTROL", "controlType":"EXPORT_RULES_CURRENT"}
{"ruleState": "CONTROL", "controlType":"CLEAR_STATE_ALL"}


##### Examles of CLI params:
--data-source kafka --rules-source kafka --alerts-sink kafka --rules-export-sink kafka

##### Special functions:
1,(active),(paymentType),,(COUNT_FLINK),(SUM),(>),(50),(20)


SQL Example:
timestamp,SQL

##### Examples:

```
2021-06-25 10:38:30,SELECT payeeId FROM source_table WHERE paymentAmount > 10
2021-06-25 10:39:30,SELECT beneficiaryId FROM source_table WHERE paymentAmount > 10
2021-06-25 10:40:30,SELECT beneficiaryId FROM source_table WHERE paymentType = 'CSH'
2021-06-25 10:41:30,SELECT SUM(paymentAmount) FROM source_table WHERE paymentAmount < 10
2021-06-25 10:42:30,SELECT paymentType, MAX(paymentAmount) FROM source_table GROUP BY paymentType
2021-06-25 10:43:30,SELECT paymentType, MIN(paymentAmount) FROM source_table GROUP BY paymentType
2021-06-25 10:44:30,SELECT t.payeeId, t.first_payment, t.second_payment FROM source_table MATCH_RECOGNIZE ( PARTITION BY payeeId ORDER BY user_action_time MEASURES FIRST(paymentAmount) AS first_payment, LAST(paymentAmount) AS second_payment ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW PATTERN (A B) DEFINE A AS paymentAmount < 100, B AS paymentAmount > 100 ) AS t
2021-06-25 10:45:30,SELECT window_start, window_end, SUM(paymentAmount) FROM TUMBLE(TABLE source_table, DESCRIPTOR(eventTime), INTERVAL '10' SECONDS) WHERE paymentAmount > 10
2021-06-25 10:45:30,SELECT window_start, window_end, SUM(paymentAmount) FROM TABLE(TUMBLE(TABLE source_table, DESCRIPTOR(user_action_time), INTERVAL '10' SECONDS)) GROUP BY window_start, window_end
```

##### Examles of CLI params:
--data-source kafka --rules-source kafka --alerts-sink kafka --rules-export-sink kafka

