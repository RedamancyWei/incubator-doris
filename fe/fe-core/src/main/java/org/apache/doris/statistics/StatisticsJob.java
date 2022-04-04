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
import org.apache.doris.common.UserException;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.Strings;
import org.glassfish.jersey.internal.guava.Sets;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

/*
Used to store statistics job info,
including job status, progress, etc.
 */
public class StatisticsJob {

    public enum JobState {
        PENDING,
        SCHEDULING,
        RUNNING,
        FINISHED,
        CANCELLED,
        FAILED
    }

    private final long id = Catalog.getCurrentCatalog().getNextId();

    /**
     * to be collected database stats.
     */
    private final long dbId;

    /**
     * to be collected table stats.
     */
    private final List<Long> tableIds;

    /**
     * to be collected column stats.
     */
    private final Map<Long, List<String>> tableIdToColumnName;

    private final Map<String, String> properties;

    /**
     * to be executed tasks.
     */
    private final List<StatisticsTask> tasks = Lists.newArrayList();

    private JobState jobState = JobState.PENDING;

    private final Date createTime = new Date(System.currentTimeMillis());
    private Date scheduleTime;
    private Date finishTime;

    public StatisticsJob(Long dbId,
                         List<Long> tableIdList,
                         Map<Long, List<String>> tableIdToColumnName,
                         Map<String, String> properties) {
        this.dbId = dbId;
        this.tableIds = tableIdList;
        this.tableIdToColumnName = tableIdToColumnName;
        this.properties = properties;
    }

    public long getId() {
        return this.id;
    }

    public long getDbId() {
        return this.dbId;
    }

    public List<Long> getTableIds() {
        return this.tableIds;
    }

    public Map<Long, List<String>> getTableIdToColumnName() {
        return this.tableIdToColumnName;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public List<StatisticsTask> getTasks() {
        return this.tasks;
    }

    public JobState getJobState() {
        return this.jobState;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public Date getCreateTime() {
        return this.createTime;
    }

    public Date getScheduleTime() {
        return this.scheduleTime;
    }

    public void setScheduleTime(Date scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public Date getFinishTime() {
        return this.finishTime;
    }

    public void setFinishTime(Date finishTime) {
        this.finishTime = finishTime;
    }

    /**
     * get statisticsJob from analyzeStmt.
     * AnalyzeStmt: analyze t1(c1,c2,c3)
     * tableId: [t1]
     * tableIdToColumnName <t1, [c1,c2,c3]>
     */
    public static StatisticsJob fromAnalyzeStmt(AnalyzeStmt analyzeStmt) throws UserException {
        List<Long> tableIdList = Lists.newArrayList();
        Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();
        List<String> columnNames = analyzeStmt.getColumnNames();

        analyzeStmt.analyze(analyzeStmt.getAnalyzer());
        String dbName = analyzeStmt.getDbName();
        String tblName = analyzeStmt.getTblName();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbName);

        if (Strings.isNullOrEmpty(tblName)) {
            List<Table> tables = db.getTables();
            for (Table table : tables) {
                long tableId = table.getId();
                tableIdList.add(tableId);
                List<String> colNames = Lists.newArrayList();
                List<Column> baseSchema = table.getBaseSchema();
                baseSchema.stream().map(Column::getName).forEach(colNames::add);
                tableIdToColumnName.put(tableId, colNames);
            }
        } else {
            Table table = db.getOlapTableOrDdlException(tblName);
            tableIdList.add(table.getId());
            tableIdToColumnName.put(table.getId(), columnNames);
        }

        return new StatisticsJob(db.getId(),
                tableIdList,
                tableIdToColumnName,
                analyzeStmt.getProperties());
    }

    public Set<Long> relatedTableId() {
        Set<Long> relatedTableId = Sets.newHashSet();
        relatedTableId.addAll(this.tableIds);
        relatedTableId.addAll(this.tableIdToColumnName.keySet());
        return relatedTableId;
    }

    public List<String> getShowInfo(@Nullable Long tableId) throws AnalysisException {
        List<String> result = Lists.newArrayList();
        result.add(Long.toString(this.id));
        result.add(this.jobState.toString());
        result.add(this.createTime.toString());
        result.add(this.scheduleTime.toString());
        result.add(this.finishTime.toString());

        int totalTaskNum = 0;
        int finishedTaskNum = 0;
        Map<Long, Set<String>> tblIdToCols = Maps.newHashMap();

        for (StatisticsTask task : this.tasks) {
            long tblId = task.getCategoryDesc().getTableId();
            if (tableId == null || tableId == tblId){
                totalTaskNum++;
                if (task.getTaskState() == StatisticsTask.TaskState.FINISHED) {
                    finishedTaskNum++;
                }
                String col = task.getCategoryDesc().getColumnName();
                if (StringUtils.isNotBlank(col)) {
                    if (tblIdToCols.containsKey(tableId)) {
                        tblIdToCols.get(tableId).add(col);
                    } else {
                        Set<String> cols = Sets.newHashSet();
                        cols.add(col);
                        tblIdToCols.put(tableId, cols);
                    }
                }
            }
        }

        // get scope
        List<String> scope = Lists.newArrayList();
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(this.dbId);
        for (Long tblId : this.tableIds) {
            Table table = db.getTableOrAnalysisException(tblId);
            List<Column> baseSchema = table.getBaseSchema();
            Set<String> cols = tblIdToCols.get(tblId);
            if (baseSchema.size() == cols.size()) {
                scope.add(table.getName() + "(*)");
            } else {
                scope.add(table.getName() + "(" + StringUtils.join(cols.toArray(), ",") + ")");
            }
        }
        result.add(StringUtils.join(scope.toArray(), ","));

        // get progress
        if (totalTaskNum == 0) {
            result.add("0");
        } else {
            result.add(finishedTaskNum + "/" + totalTaskNum);
        }

        return result;
    }
}
