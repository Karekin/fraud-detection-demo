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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableAlert {
    private String sql;
    private Boolean isAdded;
    private Object[] response;
    private Long timestamp;
    public static TableAlert fromTuple(Tuple4<String, Boolean, Row, Long> el) {
        Object[] resp = new Object[el.f2.getArity()];
        for (int i = 0; i < resp.length; i++) resp[i] = el.f2.getField(i);
        return new TableAlert(el.f0, el.f1, resp, el.f3);
    }
}
