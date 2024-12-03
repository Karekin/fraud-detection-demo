/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.field.dynamicrules;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableAlert {
    private String sql;
    private Boolean isAdded;
    private Object[] response;
    private Long timestamp;
    public static TableAlert fromTuple(Tuple3<String, RowData, Long> el) {
        RowData rowData = el.f1;
        StringData sqlData = rowData.getString(0);
        String sql = sqlData.toString();

        boolean isAdded = rowData.getBoolean(1);

        int numElements = 4;  // 假设你知道 `response` Row 包含4个字段 TODO numElements 是什么？
        RowData responseRow = rowData.getRow(2, numElements); // numElements 是这个 RowData 中字段的数量
        Object[] response = new Object[responseRow.getArity()];
        for (int i = 0; i < responseRow.getArity(); i++) {
            response[i] = responseRow.getString(i).toString();  // 假设每个子字段都是 String 类型
        }

        long timestamp = rowData.getLong(3);

        return new TableAlert(sql, isAdded, response, timestamp);
    }
}
