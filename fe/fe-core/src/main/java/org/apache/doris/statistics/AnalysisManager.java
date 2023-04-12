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

import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.analysis.ShowAnalyzeStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisTaskInfo.JobType;
import org.apache.doris.statistics.AnalysisTaskInfo.ScheduleType;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AnalysisManager {

    public final AnalysisTaskScheduler taskScheduler;

    private static final Logger LOG = LogManager.getLogger(AnalysisManager.class);

    private static final String UPDATE_JOB_STATE_SQL_TEMPLATE = "UPDATE "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.ANALYSIS_JOB_TABLE + " "
            + "SET state = '${jobState}' ${message} ${updateExecTime} WHERE job_id = ${jobId}";

    private static final String SHOW_JOB_STATE_SQL_TEMPLATE = "SELECT "
            + "job_id, catalog_name, db_name, tbl_name, col_name, job_type, "
            + "analysis_type, message, last_exec_time_in_ms, state, schedule_type "
            + "FROM " + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.ANALYSIS_JOB_TABLE;

    // The time field that needs to be displayed
    private static final String LAST_EXEC_TIME_IN_MS = "last_exec_time_in_ms";

    private final ConcurrentMap<Long, Map<Long, AnalysisTaskInfo>> analysisJobIdToTaskMap;

    private StatisticsCache statisticsCache;

    private final AnalysisTaskExecutor taskExecutor;

    public AnalysisManager() {
        analysisJobIdToTaskMap = new ConcurrentHashMap<>();
        this.taskScheduler = new AnalysisTaskScheduler();
        taskExecutor = new AnalysisTaskExecutor(taskScheduler);
        this.statisticsCache = new StatisticsCache();
        taskExecutor.start();
    }

    public StatisticsCache getStatisticsCache() {
        return statisticsCache;
    }

    public void createAnalysisJob(AnalyzeStmt analyzeStmt) {
        Map<Long, AnalysisTaskInfo> analysisTaskInfos = new HashMap<>();
        AnalysisTaskInfoBuilder taskInfoBuilder = new AnalysisTaskInfoBuilder();

        String catalogName = analyzeStmt.getCatalogName();
        String db = analyzeStmt.getDBName();
        TableName tbl = analyzeStmt.getTblName();
        StatisticsUtil.convertTableNameToObjects(tbl);
        String tblName = tbl.getTbl();
        Set<String> colNames = analyzeStmt.getColumnNames();
        Set<String> partitionNames = analyzeStmt.getPartitionNames();
        boolean isIncrement = analyzeStmt.isIncrement;
        int periodInMin = analyzeStmt.getPeriodIntervalInMin();
        int samplePercent = analyzeStmt.getSamplePercent();
        long jobId = Env.getCurrentEnv().getNextId();

        taskInfoBuilder.setJobId(jobId);
        taskInfoBuilder.setCatalogName(catalogName);
        taskInfoBuilder.setDbName(db);
        taskInfoBuilder.setTblName(tblName);
        taskInfoBuilder.setPartitionNames(partitionNames);
        taskInfoBuilder.setJobType(JobType.MANUAL);
        taskInfoBuilder.setState(AnalysisState.PENDING);
        taskInfoBuilder.setIncrement(isIncrement);

        if (periodInMin > 0) {
            long periodIntervalInMs = TimeUnit.MINUTES.toMillis(periodInMin);
            taskInfoBuilder.setperiodIntervalInMs(periodIntervalInMs);
            taskInfoBuilder.setScheduleType(ScheduleType.PERIOD);
        } else {
            taskInfoBuilder.setScheduleType(ScheduleType.ONCE);
        }

        if (samplePercent > 0) {
            taskInfoBuilder.setSamplePercent(samplePercent);
            taskInfoBuilder.setAnalysisMethod(AnalysisMethod.SAMPLE);
        } else {
            taskInfoBuilder.setAnalysisMethod(AnalysisMethod.FULL);
        }

        if (analyzeStmt.isHistogram) {
            // analyze column histogram
            taskInfoBuilder.setAnalysisType(AnalysisType.HISTOGRAM);
            int numBuckets = analyzeStmt.getNumBuckets();
            int maxBucketNum = numBuckets > 0 ? numBuckets
                    : StatisticConstants.HISTOGRAM_MAX_BUCKET_NUM;
            taskInfoBuilder.setMaxBucketNum(maxBucketNum);
            buildColumnTaskInfo(colNames, taskInfoBuilder, analysisTaskInfos);
        }

        if (!analyzeStmt.isHistogram) {
            // If the analysis is not incremental, need to delete existing statistics.
            // we cannot collect histograms incrementally and do not support it
            if (!analyzeStmt.isIncrement) {
                Env.getCurrentEnv().getAnalysisHelper().asyncExecute(() -> {
                    long dbId = analyzeStmt.getDbId();
                    TableIf table = analyzeStmt.getTable();
                    Set<Long> tblIds = Sets.newHashSet(table.getId());
                    Set<Long> partIds = partitionNames.stream()
                            .map(p -> table.getPartition(p).getId())
                            .collect(Collectors.toSet());
                    StatisticsRepository.dropStatistics(dbId, tblIds, colNames, partIds);
                });
            }

            // analyze column statistics
            taskInfoBuilder.setAnalysisType(AnalysisType.COLUMN);
            buildColumnTaskInfo(colNames, taskInfoBuilder, analysisTaskInfos);

            TableType tableType = analyzeStmt.getTable().getType();
            if (analyzeStmt.isAnalysisMv && tableType.equals(TableType.OLAP)) {
                // analyze index statistics
                taskInfoBuilder.setAnalysisType(AnalysisType.INDEX);
                OlapTable olapTable = (OlapTable) analyzeStmt.getTable();
                buildIndexTaskInfo(taskInfoBuilder, analysisTaskInfos, olapTable);
            }
        }

        // start scheduling analysis tasks
        scheduleAnalysisJob(jobId, analysisTaskInfos);
    }

    public void scheduleAnalysisJob(long jobId, Map<Long, AnalysisTaskInfo> analysisJobInfos) {
        Map<Long, AnalysisTaskInfo> existingAnalysisJobInfos = analysisJobIdToTaskMap.get(jobId);
        if (existingAnalysisJobInfos == null) {
            analysisJobIdToTaskMap.put(jobId, analysisJobInfos);
            analysisJobInfos.values().forEach(taskScheduler::schedule);
        } else {
            Map<Long, AnalysisTaskInfo> analysisTaskInfoToRun = new HashMap<>();
            for (Map.Entry<Long, AnalysisTaskInfo> entry : analysisJobInfos.entrySet()) {
                Long taskId = entry.getKey();
                AnalysisTaskInfo taskInfo = entry.getValue();
                if (existingAnalysisJobInfos.containsKey(taskId)) {
                    AnalysisState state = taskInfo.state;
                    if (state == AnalysisState.PENDING || state == AnalysisState.RUNNING) {
                        continue;
                    }
                    analysisTaskInfoToRun.put(taskId, taskInfo);
                }
            }
            analysisJobIdToTaskMap.get(jobId).putAll(analysisTaskInfoToRun);
            analysisTaskInfoToRun.values().forEach(taskScheduler::schedule);
        }
    }

    public void updateTaskStatus(AnalysisTaskInfo info, AnalysisState jobState, String message, long time) {
        Map<String, String> params = new HashMap<>();
        params.put("jobState", jobState.toString());
        params.put("message", StringUtils.isNotEmpty(message) ? String.format(", message = '%s'", message) : "");
        params.put("updateExecTime", time == -1 ? "" : ", last_exec_time_in_ms=" + time);
        params.put("jobId", String.valueOf(info.jobId));
        try {
            StatisticsUtil.execUpdate(new StringSubstitutor(params).replace(UPDATE_JOB_STATE_SQL_TEMPLATE));
        } catch (Exception e) {
            LOG.warn(String.format("Failed to update state for job: %s", info.jobId), e);
        } finally {
            info.state = jobState;
            if (analysisJobIdToTaskMap.get(info.jobId).values()
                    .stream().allMatch(i -> i.state != null
                            && i.state != AnalysisState.PENDING && i.state != AnalysisState.RUNNING)) {
                analysisJobIdToTaskMap.remove(info.jobId);
            }

        }
    }

    public List<List<Comparable>> showAnalysisJob(ShowAnalyzeStmt stmt) throws DdlException {
        String whereClause = stmt.getWhereClause();
        long limit = stmt.getLimit();
        String executeSql = SHOW_JOB_STATE_SQL_TEMPLATE
                + (whereClause.isEmpty() ? "" : " WHERE " + whereClause)
                + (limit == -1L ? "" : " LIMIT " + limit);

        List<List<Comparable>> results = Lists.newArrayList();
        ImmutableList<String> titleNames = stmt.getTitleNames();
        List<ResultRow> resultRows = StatisticsUtil.execStatisticQuery(executeSql);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        for (ResultRow resultRow : resultRows) {
            List<Comparable> result = Lists.newArrayList();
            for (String column : titleNames) {
                String value = resultRow.getColumnValue(column);
                if (LAST_EXEC_TIME_IN_MS.equals(column)) {
                    long timeMillis = Long.parseLong(value);
                    value = dateFormat.format(new Date(timeMillis));
                }
                result.add(value);
            }
            results.add(result);
        }

        return results;
    }

    private void buildColumnTaskInfo(Set<String> colNames, AnalysisTaskInfoBuilder taskInfoBuilder,
            Map<Long, AnalysisTaskInfo> analysisTaskInfos) {
        if (colNames == null || colNames.isEmpty()) {
            return;
        }

        for (String colName : colNames) {
            AnalysisTaskInfoBuilder colTaskInfoBuilder = taskInfoBuilder.deepCopy();
            long taskId = Env.getCurrentEnv().getNextId();
            AnalysisTaskInfo analysisTaskInfo = colTaskInfoBuilder.setTaskId(taskId)
                    .setColName(colName).build();
            try {
                StatisticsRepository.createAnalysisTask(analysisTaskInfo);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create analysis job", e);
            }
            analysisTaskInfos.put(taskId, analysisTaskInfo);
        }
    }

    private void buildIndexTaskInfo(AnalysisTaskInfoBuilder taskInfoBuilder,
            Map<Long, AnalysisTaskInfo> analysisTaskInfos, OlapTable olapTable) {
        try {
            olapTable.readLock();
            for (MaterializedIndexMeta meta : olapTable.getIndexIdToMeta().values()) {
                if (meta.getDefineStmt() == null) {
                    continue;
                }
                AnalysisTaskInfoBuilder indexTaskInfoBuilder = taskInfoBuilder.deepCopy();
                long taskId = Env.getCurrentEnv().getNextId();
                AnalysisTaskInfo analysisTaskInfo = indexTaskInfoBuilder.setTaskId(taskId)
                        .setIndexId(meta.getIndexId()).build();
                try {
                    StatisticsRepository.createAnalysisTask(analysisTaskInfo);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create analysis job", e);
                }
                analysisTaskInfos.put(taskId, analysisTaskInfo);
            }
        } finally {
            olapTable.readUnlock();
        }
    }
}
