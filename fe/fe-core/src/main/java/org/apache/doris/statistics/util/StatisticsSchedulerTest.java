// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.statistics.util;

import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;

import java.util.List;

/**
 * Schedule statistics task
 */
public class StatisticsSchedulerTest extends MasterDaemon {
    @Override
    protected void runAfterCatalogReady() {
        try {
            Thread.sleep(1000 * 5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String db = "test";
        String sql = "SELECT * FROM table3";

        InternalQuery query = new InternalQuery(db, sql);
        try {
            InternalQueryResult result = query.query();
            List<ResultRow> resultRows = result.getResultRows();
            for (ResultRow resultRow : resultRows) {
                List<String> columns = resultRow.getColumns();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < resultRow.getColumns().size(); i++) {
                    sb.append(resultRow.getColumnIndex(columns.get(i)));
                    sb.append(" ");
                    sb.append(resultRow.getColumnName(i));
                    sb.append(" ");
                    sb.append(resultRow.getColumnType(columns.get(i)));
                    sb.append(" ");
                    sb.append(resultRow.getColumnType(i));
                    sb.append(" ");
                    sb.append(resultRow.getColumnValue(columns.get(i)));
                    sb.append(" ");
                    sb.append(resultRow.getColumnValue(i));
                    sb.append("\n");
                }
                System.out.println(sb);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
