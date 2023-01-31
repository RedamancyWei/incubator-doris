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
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisTaskInfo.JobType;
import org.apache.doris.statistics.AnalysisTaskInfo.ScheduleType;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AnalysisManager {

    public final AnalysisTaskScheduler taskScheduler;

    private static final Logger LOG = LogManager.getLogger(AnalysisManager.class);

    private static final String UPDATE_JOB_STATE_SQL_TEMPLATE = "UPDATE "
            + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.ANALYSIS_JOB_TABLE + " "
            + "SET state = '${jobState}' ${message} ${updateExecTime} WHERE job_id = ${jobId}";

    private static final String SHOW_JOB_STATE_SQL_TEMPLATE = "SELECT "
            + "job_id, db_name, tbl_name, col_name, job_type, state, schedule_type, last_exec_time_in_ms,message "
            + "FROM " + FeConstants.INTERNAL_DB_NAME + "." + StatisticConstants.ANALYSIS_JOB_TABLE + " ";

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
        String catalogName = analyzeStmt.getCatalogName();
        String db = analyzeStmt.getDBName();
        TableName tbl = analyzeStmt.getTblName();
        StatisticsUtil.convertTableNameToObjects(tbl);
        List<String> colNames = analyzeStmt.getOptColumnNames();
        Map<Long, AnalysisTaskInfo> analysisTaskInfos = new HashMap<>();
        long jobId = Env.getCurrentEnv().getNextId();
        if (colNames != null) {
            for (String colName : colNames) {
                long taskId = Env.getCurrentEnv().getNextId();
                AnalysisType analType = analyzeStmt.isHistogram ? AnalysisType.HISTOGRAM : AnalysisType.COLUMN;
                AnalysisTaskInfo analysisTaskInfo = new AnalysisTaskInfoBuilder().setJobId(jobId)
                        .setTaskId(taskId).setCatalogName(catalogName).setDbName(db)
                        .setTblName(tbl.getTbl()).setColName(colName).setJobType(JobType.MANUAL)
                        .setAnalysisMethod(AnalysisMethod.FULL).setAnalysisType(analType)
                        .setState(AnalysisState.PENDING)
                        .setScheduleType(ScheduleType.ONCE).build();
                try {
                    StatisticsRepository.createAnalysisTask(analysisTaskInfo);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create analysis job", e);
                }
                analysisTaskInfos.put(taskId, analysisTaskInfo);
            }
        }
        if (analyzeStmt.wholeTbl && analyzeStmt.getTable().getType().equals(TableType.OLAP)) {
            OlapTable olapTable = (OlapTable) analyzeStmt.getTable();
            try {
                olapTable.readLock();
                for (MaterializedIndexMeta meta : olapTable.getIndexIdToMeta().values()) {
                    if (meta.getDefineStmt() == null) {
                        continue;
                    }
                    long taskId = Env.getCurrentEnv().getNextId();
                    AnalysisTaskInfo analysisTaskInfo = new AnalysisTaskInfoBuilder().setJobId(
                                    jobId).setTaskId(taskId)
                            .setCatalogName(catalogName).setDbName(db)
                            .setTblName(tbl.getTbl()).setIndexId(meta.getIndexId()).setJobType(JobType.MANUAL)
                            .setAnalysisMethod(AnalysisMethod.FULL).setAnalysisType(AnalysisType.INDEX)
                            .setScheduleType(ScheduleType.ONCE).build();
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
        analysisJobIdToTaskMap.put(jobId, analysisTaskInfos);
        analysisTaskInfos.values().forEach(taskScheduler::schedule);
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

        Long jobId = stmt.getJobId();
        String tblName = "stmt.getTblName";
        String state = stmt.getStateValue();

        StringBuilder whereClause = new StringBuilder();

        if (jobId != null) {
            whereClause.append("job_Id = ").append(jobId);
        }
        if (!Strings.isNullOrEmpty(tblName)) {
            whereClause.append(whereClause.length() > 0 ? " AND " : "")
                    .append("tbl_name = ").append("\"").append(tblName).append("\"");
        }
        if (!Strings.isNullOrEmpty(state)) {
            whereClause.append(whereClause.length() > 0 ? " AND " : "")
                    .append("state = ").append("\"").append(state).append("\"");
        }

        String executeSQL = SHOW_JOB_STATE_SQL_TEMPLATE;

        if (whereClause.length() > 0) {
            executeSQL = executeSQL + "WHERE " +  whereClause;
        }

        List<ResultRow> resultRows = StatisticsUtil.execStatisticQuery(executeSQL);

        List<List<Comparable>> results = Lists.newArrayList();
        ImmutableList<String> titleNames = stmt.getTitleNames();

        for (ResultRow resultRow : resultRows) {
            List<Comparable> result = Lists.newArrayList();
            for (String column : titleNames) {
                String columnValue = resultRow.getColumnValue(column);
                result.add(columnValue);
            }
            results.add(result);
        }

        return results;

        // columnDefs.add(new ColumnDef("job_id", TypeDef.create(PrimitiveType.BIGINT)));
        // columnDefs.add(new ColumnDef("task_id", TypeDef.create(PrimitiveType.BIGINT)));
        // columnDefs.add(new ColumnDef("catalog_name", TypeDef.createVarchar(1024)));
        // columnDefs.add(new ColumnDef("db_name", TypeDef.createVarchar(1024)));
        // columnDefs.add(new ColumnDef("tbl_name", TypeDef.createVarchar(1024)));
        // columnDefs.add(new ColumnDef("col_name", TypeDef.createVarchar(1024)));
        // columnDefs.add(new ColumnDef("index_id", TypeDef.create(PrimitiveType.BIGINT)));
        // columnDefs.add(new ColumnDef("job_type", TypeDef.createVarchar(32)));
        // columnDefs.add(new ColumnDef("analysis_type", TypeDef.createVarchar(32)));
        // columnDefs.add(new ColumnDef("message", TypeDef.createVarchar(1024)));
        // columnDefs.add(new ColumnDef("last_exec_time_in_ms", TypeDef.create(PrimitiveType.BIGINT)));
        // columnDefs.add(new ColumnDef("state", TypeDef.createVarchar(32)));
        // columnDefs.add(new ColumnDef("schedule_type", TypeDef.createVarchar(32)));
    }

}
