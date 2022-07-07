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
import org.apache.doris.statistics.StatisticsTaskResult.TaskResult;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
            // step1: construct query by statsDescList
            constructQuery(statsDesc);
            // step2: execute query
            // the result should be sequence by @statsTypeList
            TaskResult taskResult = executeQuery(statsDesc);
            taskResults.add(taskResult);
            // TaskResult result = createNewTaskResult(category, granularity);
            // List<StatsType> statsTypes = statsDesc.getStatsTypes();
        }

        // step3: construct StatisticsTaskResult by query result
        // constructTaskResult(queryResultList);
        return new StatisticsTaskResult(taskResults);
    }

    protected String constructQuery(StatisticsDesc statsDesc) throws DdlException {
        Map<String, String> params = getQueryParams(statsDesc);
        List<StatsType> statsTypes = statsDesc.getStatsTypes();
        for (StatsType statsType : statsTypes) {
            switch (statsType) {
                case ROW_COUNT:
                    return getRowContQuery(params);
                case NUM_NULLS:
                    return getNumNullsQuery(params);
                case MAX_SIZE:
                case AVG_SIZE:
                    return getMaxMinSizeQuery(params);
                case NDV:
                case MAX_VALUE:
                case MIN_VALUE:
                    return getNdvMaxMinValueQuery(params);
                case DATA_SIZE:
                default:
                    return null;
            }
        }
        return null;
        // TODO
        // step1: construct FROM by @granularityDesc
        // step2: construct SELECT LIST by @statsTypeList
    }

    private String getNdvMaxMinValueQuery(Map<String, String> params) {
        return null;
    }

    private String buildRowContQuery(Map<String, String> params) {
        return null;
    }

    private String getMaxMinSizeQuery(Map<String, String> params) {
        return null;
    }

    private String getNumNullsQuery(Map<String, String> params) {
        return null;
    }

    private String getRowContQuery(Map<String, String> params) {
        return null;
    }

    protected TaskResult executeQuery(StatisticsDesc statsDesc) {
        StatsCategory category = statsDesc.getCategory();
        StatsGranularity granularity = statsDesc.getGranularity();
        List<StatsType> statsTypes = statsDesc.getStatsTypes();

        List<String> queryRes = Lists.newArrayList();
        queryRes.add("1");
        assert queryRes.size() == statsDesc.getStatsTypes().size();
        TaskResult result = createNewTaskResult(category, granularity);

        IntStream.range(0, statsTypes.size()).forEach(i -> {
            StatsType statsType = statsTypes.get(i);
            String statsValue = queryRes.get(i);
            result.getStatsTypeToValue().put(statsType, statsValue);
        });

        return result;
    }

    protected StatisticsTaskResult constructTaskResult(List<String> queryResultList) {
        // TODO
        return null;
    }

    private Map<String, String> getQueryParams(StatisticsDesc statsDesc) throws DdlException {
        StatsCategory category = statsDesc.getCategory();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(category.getDbId());
        Table table =  db.getTableOrDdlException(category.getTableId());

        Map<String, String> params = Maps.newHashMap();
        params.put("database", db.getFullName());
        params.put("table", table.getName());
        params.put("partition", category.getPartitionName());
        params.put("column", category.getColumnName());
        return params;
    }
}
