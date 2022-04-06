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

package org.apache.doris.statistics;

import static org.apache.doris.statistics.StatsGranularityDesc.StatsGranularity.PARTITION;
import static org.apache.doris.statistics.StatsGranularityDesc.StatsGranularity.TABLET;

import java.io.StringReader;
import java.util.HashMap;
import org.apache.doris.analysis.SelectStmt;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.qe.OriginStatement;

/*
A statistics task that collects statistics by executing query.
The results of the query will be returned as @StatisticsTaskResult.
 */
public class SQLStatisticsTask extends StatisticsTask {

    public static final String ROW_COUNT_SQL = "select count(*) from $tableName;";
    public static final String MAX_MIN_NDV_SQL = "select max($columnName), min($columnName), ndv($columnName) from $tableName;";
    public static final String NUM_NULLS_SQL = "select count($columnName) from $tableName($partition) where $columnName is null;";
    public static final String MAX_AVG_COL_LENS_SQL = "select max(length($columnName)), avg(length($columnName)) from $tableName TABLESAMPLE;";

    private SelectStmt query;

    public SQLStatisticsTask(long jobId, StatsGranularityDesc granularityDesc,
                             StatsCategoryDesc categoryDesc, List<StatsType> statsTypeList) {
        super(jobId, granularityDesc, categoryDesc, statsTypeList);
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        // step1: construct query by statsDescList
        constructQuery();

        // step2: execute query
        // the result should be sequence by @statsTypeList
        List<String> queryResultList = executeQuery(this.query);

        // step3: construct StatisticsTaskResult by query result
        return constructTaskResult(queryResultList);
    }

    protected void constructQuery() throws AnalysisException {
        long dbId = this.getCategoryDesc().getDbId();
        long tableId = this.getCategoryDesc().getTableId();
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(dbId);
        Table table = db.getTableOrAnalysisException(tableId);
        String dbName = db.getFullName();
        String tableName = table.getName();
        String columnName = this.getCategoryDesc().getColumnName();

        // step1: construct FROM by @granularityDesc
        if (this.getGranularityDesc().getGranularity() == TABLET) {
            long tabletId = this.getGranularityDesc().getTabletId();
            tableName = tableName + "(" + tabletId + ")";
        } else if (this.getGranularityDesc().getGranularity() == PARTITION) {
            long partitionId = this.getGranularityDesc().getPartitionId();
            tableName = tableName + "(" + partitionId + ")";
        }

        HashMap<String, Object> params = Maps.newHashMap();
        params.put("dbName", dbName);
        params.put("columnName", columnName);
        params.put("tableName", tableName);

        // step2: construct SELECT LIST by @statsTypeList
        String queryString = getQueryString(params);

        try {
            // step3: construct queryStmt
            SqlParser parser = new SqlParser(new SqlScanner(new StringReader(queryString)));
            this.query = (SelectStmt) SqlParserUtils.getStmt(parser, 0);
            this.query.setOrigStmt(new OriginStatement(queryString, 0));
        } catch (Exception e) {
            LOG.warn("failed to build query", e);
        }
    }

    protected List<String> executeQuery(SelectStmt query) {
        // TODO (ML)
        return null;
    }

    protected StatisticsTaskResult constructTaskResult(List<String> queryResultList) {
        Preconditions.checkState(this.statsTypeList.size() == queryResultList.size());
        Map<StatsType, String> statsTypeToValue = Maps.newHashMap();
        for (int i = 0; i < this.statsTypeList.size(); i++) {
            statsTypeToValue.put(this.statsTypeList.get(i), queryResultList.get(i));
        }
        return new StatisticsTaskResult(this.granularityDesc, this.categoryDesc, statsTypeToValue);
    }

    public String getQueryString(HashMap<String, Object> params) {
        switch (this.getStatsTypeList().get(0)) {
            case NDV:
            case MAX_SIZE:
            case MIN_VALUE:
                return StatisticsUtils.processTemplate(MAX_MIN_NDV_SQL, params);
            case MAX_COL_LENS:
            case AVG_COL_LENS:
                return StatisticsUtils.processTemplate(MAX_AVG_COL_LENS_SQL, params);
            case NUM_NULLS:
                return StatisticsUtils.processTemplate(NUM_NULLS_SQL, params);
            case ROW_COUNT:
                return StatisticsUtils.processTemplate(ROW_COUNT_SQL, params);
            default:
                return "";
        }
    }
}
