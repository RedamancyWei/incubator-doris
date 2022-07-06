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

import org.apache.doris.statistics.StatisticsTaskResult.TaskResult;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.IntStream;

/*
A statistics task that collects statistics by executing query.
The results of the query will be returned as @StatisticsTaskResult.
 */
public class SQLStatisticsTask extends StatisticsTask {
    private String sql;

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

    protected void constructQuery(StatisticsDesc statsDesc) {
        // TODO
        // step1: construct FROM by @granularityDesc
        // step2: construct SELECT LIST by @statsTypeList
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
}
