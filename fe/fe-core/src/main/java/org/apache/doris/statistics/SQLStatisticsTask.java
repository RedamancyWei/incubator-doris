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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.InvalidFormatException;
import org.apache.doris.statistics.StatisticsTaskResult.TaskResult;
import org.apache.doris.statistics.util.QueryResultSet;
import org.apache.doris.statistics.util.SqlClient;
import org.apache.doris.statistics.util.SqlFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/*
A statistics task that collects statistics by executing query.
The results of the query will be returned as @StatisticsTaskResult.
 */
public class SQLStatisticsTask extends StatisticsTask {
    private String query;

    public SQLStatisticsTask(long jobId, List<StatisticsDesc> statsDescs) {
        super(jobId, statsDescs);
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        checkStatisticsDesc();
        List<TaskResult> taskResults = Lists.newArrayList();

        for (StatisticsDesc statsDesc : statsDescs) {
            query = constructQuery(statsDesc);
            TaskResult taskResult = executeQuery(statsDesc);
            taskResults.add(taskResult);
        }

        return new StatisticsTaskResult(taskResults);
    }

    protected String constructQuery(StatisticsDesc statsDesc) throws DdlException, InvalidFormatException {
        Map<String, String> params = getQueryParams(statsDesc);
        List<StatsType> statsTypes = statsDesc.getStatsTypes();
        String partitionName = statsDesc.getCategory().getPartitionName();

        switch (statsTypes.get(0)) {
            case ROW_COUNT:
                if (Strings.isNullOrEmpty(partitionName)) {
                    return SqlFactory.buildRowCountSql(params);
                }
                return SqlFactory.buildPartitionRowCountSql(params);
            case NUM_NULLS:
                if (Strings.isNullOrEmpty(partitionName)) {
                    return SqlFactory.buildNumNullsSql(params);
                }
                return SqlFactory.buildPartitionNumNullsSql(params);
            case MAX_SIZE:
            case AVG_SIZE:
                if (Strings.isNullOrEmpty(partitionName)) {
                    return SqlFactory.buildMaxAvgColLensSql(params);
                }
                return SqlFactory.buildPartitionMaxAvgColLensSql(params);
            case NDV:
            case MAX_VALUE:
            case MIN_VALUE:
                if (Strings.isNullOrEmpty(partitionName)) {
                    return SqlFactory.buildMinMaxNdvValueSql(params);
                }
                return SqlFactory.buildPartitionMinMaxNdvValueSql(params);
            case DATA_SIZE:
            default:
                throw new DdlException("Unsupported statistics type(" + statsTypes.get(0) + ").");
        }
    }

    protected TaskResult executeQuery(StatisticsDesc statsDesc)
            throws DdlException, IOException, NoSuchAlgorithmException {
        StatsGranularity granularity = statsDesc.getGranularity();
        List<StatsType> statsTypes = statsDesc.getStatsTypes();
        StatsCategory category = statsDesc.getCategory();

        String dbName = Catalog.getCurrentCatalog()
                .getDbOrDdlException(category.getDbId()).getFullName().split(":")[1];
        SqlClient sqlClient = new SqlClient(dbName);
        // TODO
        sqlClient.init();
        QueryResultSet query = sqlClient.query(this.query);
        List<List<Object>> rows = query.getRows();

        if (rows.size() == 1) {
            List<Object> row = rows.get(0);
            assert row.size() == statsTypes.size();
            TaskResult result = createNewTaskResult(category, granularity);
            IntStream.range(0, statsTypes.size()).forEach(i -> {
                StatsType statsType = statsTypes.get(i);
                String statsValue = (String) row.get(i);
                result.getStatsTypeToValue().put(statsType, statsValue);
            });
            return result;
        } else {
            throw new DdlException("Statistics query result is incorrect, " + rows);
        }
    }

    private Map<String, String> getQueryParams(StatisticsDesc statsDesc) throws DdlException {
        StatsCategory category = statsDesc.getCategory();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(category.getDbId());
        Table table = db.getTableOrDdlException(category.getTableId());

        Map<String, String> params = Maps.newHashMap();
        params.put("database", db.getFullName());
        params.put("table", table.getName());
        params.put("partition", category.getPartitionName());
        params.put("column", category.getColumnName());
        return params;
    }
}
