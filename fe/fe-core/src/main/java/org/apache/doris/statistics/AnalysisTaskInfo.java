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

import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class AnalysisTaskInfo {

    private static final Logger LOG = LogManager.getLogger(AnalysisTaskInfo.class);


    public enum AnalysisMethod {
        SAMPLE,
        FULL
    }

    public enum AnalysisType {
        COLUMN,
        INDEX,
        HISTOGRAM
    }

    public enum JobType {
        // submit by user directly
        MANUAL,
        // submit by system automatically
        SYSTEM
    }

    public enum ScheduleType {
        ONCE,
        PERIOD
    }

    public final long jobId;

    public final long taskId;

    public final String catalogName;

    public final String dbName;

    public final String tblName;

    public final String colName;

    public final Set<String> partitionNames;

    public final Long indexId;

    public final JobType jobType;

    public final AnalysisMethod analysisMethod;

    public final AnalysisType analysisType;

    public final boolean isIncrement;

    public final Long periodIntervalInMs;

    public final Integer samplePercent;

    public final Integer maxBucketNum;

    public String message;

    // finished or failed
    public Long lastExecTimeInMs;

    public AnalysisState state;

    public final ScheduleType scheduleType;

    public AnalysisTaskInfo(long jobId, long taskId, String catalogName, String dbName, String tblName, String colName,
            Set<String> partitionNames, Long indexId, JobType jobType, AnalysisMethod analysisMethod,
            AnalysisType analysisType, boolean isIncrement, Long periodIntervalInMs, Integer samplePercent, Integer maxBucketNum,
            String message, Long lastExecTimeInMs, AnalysisState state, ScheduleType scheduleType) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tblName = tblName;
        this.colName = colName;
        this.partitionNames = partitionNames;
        this.indexId = indexId;
        this.jobType = jobType;
        this.analysisMethod = analysisMethod;
        this.analysisType = analysisType;
        this.isIncrement = isIncrement;
        this.periodIntervalInMs = periodIntervalInMs;
        this.samplePercent = samplePercent;
        this.maxBucketNum = maxBucketNum;
        this.message = message;
        this.lastExecTimeInMs = lastExecTimeInMs;
        this.state = state;
        this.scheduleType = scheduleType;
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner("\n", getClass().getName() + ":\n", "\n");
        sj.add("JobId: " + jobId);
        sj.add("CatalogName: " + catalogName);
        sj.add("DBName: " + dbName);
        sj.add("TableName: " + tblName);
        sj.add("ColumnName: " + colName);
        sj.add("PartitionNames: " + partitionNames);
        sj.add("TaskType: " + analysisType.toString());
        sj.add("TaskMethod: " + analysisMethod.toString());
        sj.add("Message: " + message);
        sj.add("LastExecTime: " + lastExecTimeInMs);
        sj.add("CurrentState: " + state.toString());
        return sj.toString();
    }

    public AnalysisState getState() {
        return state;
    }

    // TODO: use thrift
    public static AnalysisTaskInfo fromResultRow(ResultRow resultRow) {
        try {
            AnalysisTaskInfoBuilder analysisTaskInfoBuilder = new AnalysisTaskInfoBuilder();
            long jobId = Long.parseLong(resultRow.getColumnValue("job_id"));
            analysisTaskInfoBuilder.setJobId(jobId);
            long taskId = Long.parseLong(resultRow.getColumnValue("task_id"));
            analysisTaskInfoBuilder.setTaskId(taskId);
            String catalogName = resultRow.getColumnValue("catalog_name");
            analysisTaskInfoBuilder.setCatalogName(catalogName);
            String dbName = resultRow.getColumnValue("db_name");
            analysisTaskInfoBuilder.setDbName(dbName);
            String tblName = resultRow.getColumnValue("tbl_name");
            analysisTaskInfoBuilder.setTblName(tblName);
            long indexId = Long.parseLong(resultRow.getColumnValue("index_id"));
            analysisTaskInfoBuilder.setIndexId(indexId);
            String colName = resultRow.getColumnValue("col_name");
            analysisTaskInfoBuilder.setColName(colName);
            String partitionNames = resultRow.getColumnValue("partition_names");
            if (partitionNames != null) {
                String[] arr = partitionNames.split(",");
                Set<String> names = Arrays.stream(arr).collect(Collectors.toSet());
                analysisTaskInfoBuilder.setPartitionNames(names);
            } else {
                analysisTaskInfoBuilder.setPartitionNames(Collections.singleton(tblName));
            }
            String jobType = resultRow.getColumnValue("job_type");
            analysisTaskInfoBuilder.setJobType(JobType.valueOf(jobType));
            String analysisMethod = resultRow.getColumnValue("analysis_method");
            analysisTaskInfoBuilder.setAnalysisMethod(AnalysisMethod.valueOf(analysisMethod));
            String analysisType = resultRow.getColumnValue("analysis_type");
            analysisTaskInfoBuilder.setAnalysisType(AnalysisType.valueOf(analysisType));
            boolean isIncrement = Boolean.parseBoolean(resultRow.getColumnValue("is_increment"));
            analysisTaskInfoBuilder.setIncrement(isIncrement);
            long periodIntervalInMs = Long.parseLong(resultRow.getColumnValue("period_interval_in_ms"));
            analysisTaskInfoBuilder.setperiodIntervalInMs(periodIntervalInMs);
            int samplePercent = Integer.parseInt(resultRow.getColumnValue("sample_percent"));
            analysisTaskInfoBuilder.setSamplePercent(samplePercent);
            int maxBucketNum = Integer.parseInt(resultRow.getColumnValue("max_bucket_num"));
            analysisTaskInfoBuilder.setMaxBucketNum(maxBucketNum);
            String message = resultRow.getColumnValue("message");
            analysisTaskInfoBuilder.setMessage(message);
            long lastExecTimeInMs = Long.parseLong(resultRow.getColumnValue("last_exec_time_in_ms"));
            analysisTaskInfoBuilder.setLastExecTimeInMs(lastExecTimeInMs);
            String state = resultRow.getColumnValue("state");
            analysisTaskInfoBuilder.setState(AnalysisState.valueOf(state));
            String scheduleType = resultRow.getColumnValue("schedule_type");
            analysisTaskInfoBuilder.setScheduleType(ScheduleType.valueOf(scheduleType));
            return analysisTaskInfoBuilder.build();
        } catch (Exception e) {
            LOG.warn("Failed to deserialize analysis task info.", e);
            return null;
        }
    }
}
