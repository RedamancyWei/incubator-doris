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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.ColumnStats;

import com.google.common.collect.ImmutableList;

import org.apache.parquet.Strings;

public class ShowColumnStatsStmt extends ShowStmt {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("column_name")
                    .add(ColumnStats.NDV.getValue())
                    .add(ColumnStats.AVG_SIZE.getValue())
                    .add(ColumnStats.MAX_SIZE.getValue())
                    .add(ColumnStats.NUM_NULLS.getValue())
                    .add(ColumnStats.MIN_VALUE.getValue())
                    .add(ColumnStats.MAX_VALUE.getValue())
                    .build();

    private TableName tableName;
    private String partitionName;

    public ShowColumnStatsStmt(TableName tableName, String partitionName) {
        this.tableName = tableName;
        this.partitionName = partitionName;
    }

    public TableName getTableName() {
        return tableName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        tableName.analyze(analyzer);

        if (!Strings.isNullOrEmpty(partitionName)) {
            Database db = analyzer.getCatalog().getDbOrAnalysisException(tableName.getDb());
            Table table = db.getTableOrAnalysisException(tableName.getTbl());
            Partition partition = table.getPartition(partitionName);
            if (partition == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_PARTITION, partitionName);
            }
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }
}
