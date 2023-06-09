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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringJoiner;

public class AnalysisJobInfo2 extends AnalysisInfo2 {

    public final long periodTimeInMs;

    public final Map<String, Set<Long>> colToPartitions;

    public static class Builder extends AnalysisInfo2.Builder<Builder> {
        protected long periodTimeInMs;
        private Map<String, Set<Long>> colToPartitions;

        public Builder() {
        }

        public Builder(AnalysisJobInfo2 jobInfo) {
            super(jobInfo);
            this.periodTimeInMs = jobInfo.periodTimeInMs;
            this.colToPartitions = jobInfo.colToPartitions;
        }

        public Builder periodTimeInMs(long periodTimeInMs) {
            this.periodTimeInMs = periodTimeInMs;
            return this;
        }

        public Builder colToPartitions(Map<String, Set<Long>> colToPartitions) {
            this.colToPartitions = colToPartitions;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public AnalysisJobInfo2 build() {
            return new AnalysisJobInfo2(this);
        }
    }

    private AnalysisJobInfo2(Builder builder) {
        super(builder);
        this.periodTimeInMs = builder.periodTimeInMs;
        this.colToPartitions = builder.colToPartitions;
    }

    // TODO: use thrift
    public static AnalysisJobInfo2 fromResultRow(ResultRow resultRow) throws DdlException {
        long jobId = Long.parseLong(resultRow.getColumnValue("job_id"));
        String catalogName = resultRow.getColumnValue("catalog_name");
        String dbName = resultRow.getColumnValue("db_name");
        String tblName = resultRow.getColumnValue("tbl_name");
        long indexId = Long.parseLong(resultRow.getColumnValue("index_id"));
        String partitionNames = resultRow.getColumnValue("partitions");
        Map<String, Set<Long>> colToPartitions = StatisticsUtil.getColToPartition(partitionNames);
        JobType jobType = JobType.valueOf(resultRow.getColumnValue("job_type"));
        AnalysisType analysisType = AnalysisType.valueOf(resultRow.getColumnValue("analysis_type"));
        AnalysisMode analysisMode = AnalysisMode.valueOf(resultRow.getColumnValue("analysis_mode"));
        AnalysisMethod analysisMethod = AnalysisMethod.valueOf(resultRow.getColumnValue("analysis_method"));
        ScheduleType scheduleType = ScheduleType.valueOf(resultRow.getColumnValue("schedule_type"));
        AnalysisState state = AnalysisState.valueOf(resultRow.getColumnValue("state"));
        String samplePercentStr = resultRow.getColumnValue("sample_percent");
        int samplePercent = StatisticsUtil.convertStrToInt(samplePercentStr);
        String sampleRowsStr = resultRow.getColumnValue("sample_rows");
        int sampleRows = StatisticsUtil.convertStrToInt(sampleRowsStr);
        String maxBucketNumStr = resultRow.getColumnValue("max_bucket_num");
        int maxBucketNum = StatisticsUtil.convertStrToInt(maxBucketNumStr);
        String periodTimeInMsStr = resultRow.getColumnValue("period_time_in_ms");
        int periodTimeInMs = StatisticsUtil.convertStrToInt(periodTimeInMsStr);
        String lastExecTimeInMsStr = resultRow.getColumnValue("last_exec_time_in_ms");
        long lastExecTimeInMs = StatisticsUtil.convertStrToLong(lastExecTimeInMsStr);
        String message = resultRow.getColumnValue("message");

        return new AnalysisJobInfo2.Builder()
                .jobId(jobId)
                .catalogName(catalogName)
                .dbName(dbName)
                .tblName(tblName)
                .indexId(indexId)
                .colToPartitions(colToPartitions)
                .jobType(jobType)
                .analysisMode(analysisMode)
                .analysisMethod(analysisMethod)
                .analysisType(analysisType)
                .scheduleType(scheduleType)
                .state(state)
                .samplePercent(samplePercent)
                .sampleRows(sampleRows)
                .maxBucketNum(maxBucketNum)
                .periodTimeInMs(periodTimeInMs)
                .lastExecTimeInMs(lastExecTimeInMs)
                .message(message)
                .build();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(jobId);
        Text.writeString(out, catalogName);
        Text.writeString(out, dbName);
        Text.writeString(out, tblName);
        out.writeLong(indexId);
        Text.writeString(out, jobType.toString());
        Text.writeString(out, analysisMode.toString());
        Text.writeString(out, analysisMethod.toString());
        Text.writeString(out, analysisType.toString());
        out.writeInt(samplePercent);
        out.writeInt(sampleRows);
        out.writeInt(maxBucketNum);
        out.writeLong(periodTimeInMs);
        out.writeLong(lastExecTimeInMs);
        Text.writeString(out, state.toString());
        Text.writeString(out, scheduleType.toString());
        Text.writeString(out, message);
        out.writeInt(colToPartitions.size());
        for (Entry<String, Set<Long>> entry : colToPartitions.entrySet()) {
            Text.writeString(out, entry.getKey());
            out.writeInt(entry.getValue().size());
            for (long partId : entry.getValue()) {
                out.writeLong(partId);
            }
        }
    }

    public static AnalysisJobInfo2 read(DataInput dataInput) throws IOException {
        long jobId = dataInput.readLong();
        String catalogName = Text.readString(dataInput);
        String dbName = Text.readString(dataInput);
        String tblName = Text.readString(dataInput);
        long indexId = dataInput.readLong();
        JobType jobType = JobType.valueOf(Text.readString(dataInput));
        AnalysisMode analysisMode = AnalysisMode.valueOf(Text.readString(dataInput));
        AnalysisMethod analysisMethod = AnalysisMethod.valueOf(Text.readString(dataInput));
        AnalysisType analysisType = AnalysisType.valueOf(Text.readString(dataInput));
        AnalysisState state = AnalysisState.valueOf(Text.readString(dataInput));
        ScheduleType scheduleType = ScheduleType.valueOf(Text.readString(dataInput));
        int samplePercent = dataInput.readInt();
        int sampleRows = dataInput.readInt();
        int maxBucketNum = dataInput.readInt();
        long periodTimeInMs = dataInput.readLong();
        long lastExecTimeInMs = dataInput.readLong();
        String message = Text.readString(dataInput);
        int size = dataInput.readInt();
        Map<String, Set<Long>> colToPartitions = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String k = Text.readString(dataInput);
            int partSize = dataInput.readInt();
            Set<Long> parts = new HashSet<>();
            for (int j = 0; j < partSize; j++) {
                parts.add(dataInput.readLong());
            }
            colToPartitions.put(k, parts);
        }

        return new AnalysisJobInfo2.Builder()
                .jobId(jobId)
                .catalogName(catalogName)
                .dbName(dbName)
                .tblName(tblName)
                .indexId(indexId)
                .colToPartitions(colToPartitions)
                .jobType(jobType)
                .analysisMode(analysisMode)
                .analysisMethod(analysisMethod)
                .analysisType(analysisType)
                .scheduleType(scheduleType)
                .state(state)
                .samplePercent(samplePercent)
                .sampleRows(sampleRows)
                .maxBucketNum(maxBucketNum)
                .periodTimeInMs(periodTimeInMs)
                .lastExecTimeInMs(lastExecTimeInMs)
                .message(message)
                .build();
    }

    @Override
    public String toString() {
        String baseString = super.toString();
        StringJoiner sj = new StringJoiner("\n", baseString, "\n");
        if (periodTimeInMs > 0) {
            sj.add("periodTimeInMs: " + StatisticsUtil.getReadableTime(periodTimeInMs));
        }
        if (colToPartitions != null) {
            sj.add("colToPartitions: " + StatisticsUtil.getColToPartitionStr(colToPartitions));
        }
        return sj.toString();
    }
}
