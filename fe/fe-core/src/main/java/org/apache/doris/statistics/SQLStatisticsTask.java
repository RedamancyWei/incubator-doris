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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.InvalidFormatException;
import org.apache.doris.statistics.StatisticsTaskResult.TaskResult;
import org.apache.doris.statistics.StatisticsTaskScheduler.ConnectionPool;
import org.apache.doris.statistics.StatsGranularity.Granularity;
import org.apache.doris.statistics.util.QueryResultSet;
import org.apache.doris.statistics.util.SqlClient;
import org.apache.doris.statistics.util.SqlFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/*
A statistics task that collects statistics by executing query.
The results of the query will be returned as @StatisticsTaskResult.
 */
public class SQLStatisticsTask extends StatisticsTask {
    private String statement;

    public SQLStatisticsTask(long jobId, List<StatisticsDesc> statsDescs) {
        super(jobId, statsDescs);
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        checkStatisticsDesc();
        List<TaskResult> taskResults = Lists.newArrayList();

        for (StatisticsDesc statsDesc : statsDescs) {
            statement = constructQuery(statsDesc);
            TaskResult taskResult = executeQuery(statsDesc);
            taskResults.add(taskResult);
        }

        return new StatisticsTaskResult(taskResults);
    }

    protected String constructQuery(StatisticsDesc statsDesc) throws DdlException, InvalidFormatException {
        Map<String, String> params = getQueryParams(statsDesc);
        List<StatsType> statsTypes = statsDesc.getStatsTypes();
        StatsType type = statsTypes.get(0);

        StatsGranularity statsGranularity = statsDesc.getStatsGranularity();
        Granularity granularity = statsGranularity.getGranularity();
        boolean nonPartitioned = granularity != Granularity.PARTITION;

        switch (type) {
            case ROW_COUNT:
                return nonPartitioned ? SqlFactory.buildRowCountSql(params)
                        : SqlFactory.buildPartitionRowCountSql(params);
            case NUM_NULLS:
                return nonPartitioned ? SqlFactory.buildNumNullsSql(params)
                        : SqlFactory.buildPartitionNumNullsSql(params);
            case MAX_SIZE:
            case AVG_SIZE:
                return nonPartitioned ? SqlFactory.buildMaxAvgSizeSql(params)
                        : SqlFactory.buildPartitionMaxAvgSizeSql(params);
            case NDV:
            case MAX_VALUE:
            case MIN_VALUE:
                return nonPartitioned ? SqlFactory.buildMinMaxNdvValueSql(params)
                        : SqlFactory.buildPartitionMinMaxNdvValueSql(params);
            case DATA_SIZE:
            default:
                throw new DdlException("Unsupported statistics type: " + type);
        }
    }

    protected TaskResult executeQuery(StatisticsDesc statsDesc)
            throws DdlException, IOException, NoSuchAlgorithmException, InterruptedException, TimeoutException {
        StatsGranularity granularity = statsDesc.getStatsGranularity();
        List<StatsType> statsTypes = statsDesc.getStatsTypes();
        StatsCategory category = statsDesc.getStatsCategory();

        String dbName = Catalog.getCurrentCatalog()
                .getDbOrDdlException(category.getDbId()).getFullName().split(":")[1];
        ConnectionPool connectionPool = Catalog.getCurrentCatalog().getStatisticsTaskScheduler()
                .getConnectionPool(dbName);
        if (connectionPool == null) {
            throw new DdlException("Unable to connect to database: " + dbName);
        }

        QueryResultSet query;
        List<List<Object>> rows;
        SqlClient sqlClient = null;

        try {
            sqlClient = connectionPool.getConnection(Config.max_cbo_statistics_task_timeout_sec);
            query = sqlClient.query(statement);
            rows = query.getRows();

            if (rows.size() == 1) {
                List<Object> row = rows.get(0);
                assert row.size() == statsTypes.size();
                TaskResult result = createNewTaskResult(category, granularity);
                List<String> columns = query.getColumns();
                for (int i = 0; i < columns.size(); i++) {
                    StatsType statsType = StatsType.fromString(columns.get(i));
                    if (row.get(i) != null) {
                        result.getStatsTypeToValue().put(statsType, String.valueOf(row.get(i)));
                    }
                }
                return result;
            }

            // Statistics statements are executed singly and return only one row data
            throw new DdlException("Statistics query result is incorrect, " + rows);
        } finally {
            if (sqlClient != null) {
                connectionPool.close(sqlClient);
            }
        }
    }

    private Map<String, String> getQueryParams(StatisticsDesc statsDesc) throws DdlException {
        StatsCategory category = statsDesc.getStatsCategory();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(category.getDbId());
        Table table = db.getTableOrDdlException(category.getTableId());

        Map<String, String> params = Maps.newHashMap();
        params.put("table", table.getName());
        params.put("partition", category.getPartitionName());
        params.put("column", category.getColumnName());
        return params;
    }
}
