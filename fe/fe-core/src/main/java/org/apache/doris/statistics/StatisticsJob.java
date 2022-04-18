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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.TimeUtils;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glassfish.jersey.internal.guava.Sets;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

/***
 * Used to store statistics job info,
 * including job status, progress, etc.
 */
public class StatisticsJob {
    private static final Logger LOG = LogManager.getLogger(StatisticsJob.class);

    public enum JobState {
        PENDING,
        SCHEDULING,
        RUNNING,
        FINISHED,
        FAILED,
        CANCELLED
    }

    private long id = Catalog.getCurrentCatalog().getNextId();;

    /**
     * to be collected database stats.
     */
    private final long dbId;

    /**
     * to be collected table stats.
     */
    private final Set<Long> tblIds;

    /**
     * to be collected column stats.
     */
    private final Map<Long, List<String>> tableIdToColumnName;

    /**
     * timeout of a collection task
     */
    private long taskTimeout;

    /**
     * to be executed tasks.
     */
    private List<StatisticsTask> tasks = Lists.newArrayList();

    private JobState jobState = JobState.PENDING;
    private final List<String> errorMsgs  = Lists.newArrayList();

    private final long createTime = System.currentTimeMillis();
    private long startTime = -1L;
    private long finishTime = -1L;
    private int progress = 0;

    public StatisticsJob(Long dbId,
                         Set<Long> tblIds,
                         Map<Long, List<String>> tableIdToColumnName) {
        this.dbId = dbId;
        this.tblIds = tblIds;
        this.tableIdToColumnName = tableIdToColumnName;
    }

    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getDbId() {
        return this.dbId;
    }

    public Set<Long> getTblIds() {
        return this.tblIds;
    }

    public Map<Long, List<String>> getTableIdToColumnName() {
        return this.tableIdToColumnName;
    }

    public long getTaskTimeout() {
        return taskTimeout;
    }

    public List<StatisticsTask> getTasks() {
        return this.tasks;
    }

    public void setTasks(List<StatisticsTask> tasks) {
        this.tasks = tasks;
    }

    public List<String> getErrorMsgs() {
        return errorMsgs;
    }

    public JobState getJobState() {
        return this.jobState;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public long getCreateTime() {
        return this.createTime;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getFinishTime() {
        return this.finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public int getProgress() {
        return this.progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    /**
     * get statisticsJob from analyzeStmt.
     * AnalyzeStmt: analyze t1(c1,c2,c3)
     * tableId: [t1]
     * tableIdToColumnName <t1, [c1,c2,c3]>
     */
    public static StatisticsJob fromAnalyzeStmt(AnalyzeStmt analyzeStmt) throws AnalysisException {
        long dbId = analyzeStmt.getDbId();
        Map<Long, List<String>> tableIdToColumnName = analyzeStmt.getTableIdToColumnName();
        Set<Long> tblIds = analyzeStmt.getTblIds();
        Map<String, String> properties = analyzeStmt.getProperties();
        StatisticsJob statisticsJob = new StatisticsJob(dbId, tblIds, tableIdToColumnName);
        statisticsJob.setOptional(analyzeStmt);
        return statisticsJob;
    }

    public List<Comparable> getShowInfo(@Nullable Long tableId) throws AnalysisException {
        List<Comparable> result = Lists.newArrayList();

        result.add(Long.toString(this.id));

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        result.add(TimeUtils.longToTimeString(this.createTime, dateFormat));
        result.add(this.startTime != -1L ? TimeUtils.longToTimeString(this.startTime, dateFormat) : "N/A");
        result.add(this.finishTime != -1L ? TimeUtils.longToTimeString(this.finishTime, dateFormat) : "N/A");

        StringBuilder sb = new StringBuilder();
        for (String errorMsg : this.errorMsgs) {
            sb.append(errorMsg).append("\n");
        }
        result.add(sb.toString());

        int totalTaskNum = 0;
        int finishedTaskNum = 0;
        Map<Long, Set<String>> tblIdToCols = Maps.newHashMap();

        for (StatisticsTask task : this.tasks) {
            long tblId = task.getCategoryDesc().getTableId();
            if (tableId == null || tableId == tblId) {
                totalTaskNum++;
                if (task.getTaskState() == StatisticsTask.TaskState.FINISHED) {
                    finishedTaskNum++;
                }
                String col = task.getCategoryDesc().getColumnName();
                if (!Strings.isNullOrEmpty(col)) {
                    if (tblIdToCols.containsKey(tblId)) {
                        tblIdToCols.get(tblId).add(col);
                    } else {
                        Set<String> cols = Sets.newHashSet();
                        cols.add(col);
                        tblIdToCols.put(tblId, cols);
                    }
                }
            }
        }

        List<String> scope = Lists.newArrayList();
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(this.dbId);
        for (Long tblId : this.tblIds) {
            try {
                Table table = db.getTableOrAnalysisException(tblId);
                List<Column> baseSchema = table.getBaseSchema();
                Set<String> cols = tblIdToCols.get(tblId);
                if (cols != null) {
                    if (baseSchema.size() == cols.size()) {
                        scope.add(table.getName() + "(*)");
                    } else {
                        scope.add(table.getName() + "(" + StringUtils.join(cols.toArray(), ",") + ")");
                    }
                }
            } catch (AnalysisException e) {
                // catch this exception when table is dropped
                LOG.info("get table failed, tableId: " + tblId, e);
            }
        }

        result.add(StringUtils.join(scope.toArray(), ","));
        result.add(finishedTaskNum + "/" + totalTaskNum);

        if (totalTaskNum == finishedTaskNum) {
            result.add("FINISHED");
        } else {
            result.add(this.jobState.toString());
        }

        return result;
    }

    private void setOptional(AnalyzeStmt stmt) {
        if (stmt.getTaskTimeout() != -1) {
            this.taskTimeout = stmt.getTaskTimeout();
        }
    }
}
