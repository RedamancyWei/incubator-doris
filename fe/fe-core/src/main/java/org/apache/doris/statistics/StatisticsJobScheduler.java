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

import com.google.common.collect.Lists;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Schedule statistics job.
 * 1. divide job to multi task
 * 2. submit all task to StatisticsTaskScheduler
 * Switch job state from pending to scheduling.
 */
public class StatisticsJobScheduler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticsJobScheduler.class);

    /**
     * If the table row-count is greater than the maximum number of Be scans for a single BE,
     * we'll divide subtasks by partition. relevant values(3700000000L&600000000L) are derived from test.
     * COUNT_MAX_SCAN_PER_TASK is for count(expr), NDV_MAX_SCAN_PER_TASK is for min(c1)/max(c1)/ndv(c1).
     */
    private static final long COUNT_MAX_SCAN_PER_TASK = 3700000000L;
    private static final long NDV_MAX_SCAN_PER_TASK = 600000000L;

    /**
     * Different statistics need to be collected for the jobs submitted by users.
     * if all statistics be collected at the same time, the cluster may be overburdened
     * and normal query services may be affected. Therefore, we put the jobs into the queue
     * and schedule them one by one, and finally divide each job to several subtasks and execute them.
     */
    public final Queue<StatisticsJob> pendingJobQueue = Queues.newLinkedBlockingQueue(Config.cbo_max_statistics_job_num);

    public StatisticsJobScheduler() {
        super("Statistics job scheduler", 0);
    }

    @Override
    protected void runAfterCatalogReady() {
        StatisticsJob pendingJob = pendingJobQueue.peek();
        if (pendingJob != null) {
            try {
                if (pendingJob.getTasks().size() == 0) {
                    divide(pendingJob);
                }
                List<StatisticsTask> tasks = pendingJob.getTasks();
                Catalog.getCurrentCatalog().getStatisticsTaskScheduler().addTasks(tasks);
                pendingJob.updateJobState(StatisticsJob.JobState.SCHEDULING);
                pendingJobQueue.remove();
            } catch (IllegalStateException e) {
                // throw IllegalStateException if the queue is full, re-add the tasks next time
                LOG.info("The statistics task queue is full, schedule the job(id={}) later", pendingJob.getId());
            } catch (DdlException e) {
                pendingJobQueue.remove();
                try {
                    // TODO change to without exception
                    pendingJob.updateJobState(StatisticsJob.JobState.FAILED);
                } catch (DdlException ddlException) {
                    LOG.fatal(ddlException.getMessage(), e);
                }
                LOG.info("Failed to schedule the statistical job(id={})", pendingJob.getId(), e);
            }
        }
    }

    public void addPendingJob(StatisticsJob statisticsJob) throws IllegalStateException {
        pendingJobQueue.add(statisticsJob);
    }

    /**
     * Statistics tasks are of the following types：
     * table:
     * - row_count: table row count are critical in estimating cardinality and memory usage of scan nodes.
     * - data_size: table size, not applicable to CBO, mainly used to monitor and manage table size.
     * column:
     * - num_distinct_value: used to determine the selectivity of an equivalent expression.
     * - min: The minimum value.
     * - max: The maximum value.
     * - num_nulls: number of nulls.
     * - avg_col_len: the average length of a column, in bytes, is used for memory and network IO evaluation.
     * - max_col_len: the Max length of the column, in bytes, is used for memory and network IO evaluation.
     * <p>
     * Divide:
     * - min, max, ndv: These three full indicators are collected by a sub-task.
     * - max_col_lens, avg_col_lens: Two sampling indicators were collected by a sub-task.
     * <p>
     * If the table row-count is greater than the maximum number of Be scans for a single BE,
     * we'll divide subtasks by partition. relevant values(3700000000L&600000000L) are derived from test.
     * <p>
     * Eventually, we will get several subtasks of the following types:
     *
     * @throws DdlException DdlException
     * @see MetaStatisticsTask
     * @see SampleSQLStatisticsTask
     * @see SQLStatisticsTask
     */
    private void divide(StatisticsJob statisticsJob) throws DdlException {
        long jobId = statisticsJob.getId();
        long dbId = statisticsJob.getDbId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        Set<Long> tblIds = statisticsJob.getTblIds();
        Map<Long, List<String>> tableIdToColumnName = statisticsJob.getTableIdToColumnName();
        List<StatisticsTask> tasks = statisticsJob.getTasks();
        List<Long> backendIds = Catalog.getCurrentSystemInfo().getBackendIds(true);

        for (Long tblId : tblIds) {
            Table tbl = db.getTableOrDdlException(tblId);
            long rowCount = tbl.getRowCount();
            List<Long> partitionIds = ((OlapTable) tbl).getPartitionIds();
            List<String> columnNameList = tableIdToColumnName.get(tblId);
            // all meta statistics are collected by a single meta statistics task
            MetaStatisticsTask metaStatsTask = new MetaStatisticsTask(jobId, Lists.newArrayList());

            // step 1: generate data_size task
            StatsCategory dataSizeCategory = getTblStatsCategory(dbId, tblId);
            StatsGranularity dataSizeGranularity = getTblStatsGranularity(tblId);
            StatisticsDesc dsStatsDesc = new StatisticsDesc(dataSizeCategory,
                dataSizeGranularity, Collections.singletonList(StatsType.DATA_SIZE));
            metaStatsTask.getStatsDescs().add(dsStatsDesc);

            // step 2: generate row_count task
            KeysType keysType = ((OlapTable) tbl).getKeysType();
            if (keysType == KeysType.DUP_KEYS) {
                StatsCategory rowCountCategory = getTblStatsCategory(dbId, tblId);
                StatsGranularity rowCountGranularity = getTblStatsGranularity(tblId);
                StatisticsDesc rowCountStatsDesc = new StatisticsDesc(rowCountCategory,
                    rowCountGranularity, Collections.singletonList(StatsType.ROW_COUNT));
                metaStatsTask.getStatsDescs().add(rowCountStatsDesc);
            } else {
                if (rowCount > backendIds.size() * COUNT_MAX_SCAN_PER_TASK) {
                    // divide subtasks by partition
                    for (Long partitionId : partitionIds) {
                        StatsCategory rowCountCategory = getTblStatsCategory(dbId, tblId);
                        StatsGranularity rowCountGranularity = getPartitionStatsGranularity(tblId, partitionId);
                        StatisticsDesc rowCountStatsDesc = new StatisticsDesc(rowCountCategory,
                            rowCountGranularity, Collections.singletonList(StatsType.ROW_COUNT));
                        SQLStatisticsTask sqlTask = new SQLStatisticsTask(jobId, Collections.singletonList(rowCountStatsDesc));
                        tasks.add(sqlTask);
                    }
                } else {
                    StatsCategory rowCountCategory = getTblStatsCategory(dbId, tblId);
                    StatsGranularity rowCountGranularity = getTblStatsGranularity(tblId);
                    StatisticsDesc rowCountStatsDesc = new StatisticsDesc(rowCountCategory,
                        rowCountGranularity, Collections.singletonList(StatsType.ROW_COUNT));
                    SQLStatisticsTask sqlTask = new SQLStatisticsTask(jobId,
                        Collections.singletonList(rowCountStatsDesc));
                    tasks.add(sqlTask);
                }
            }

            // step 3: generate [min,max,ndv] task
            if (rowCount > backendIds.size() * NDV_MAX_SCAN_PER_TASK) {
                // divide subtasks by partition
                columnNameList.forEach(columnName -> {
                    for (Long partitionId : partitionIds) {
                        StatsCategory columnCategory = getColStatsCategory(dbId, tblId, columnName);
                        StatsGranularity columnGranularity = getPartitionStatsGranularity(tblId, partitionId);
                        List<StatsType> statsTypes = Arrays.asList(StatsType.MIN_VALUE, StatsType.MAX_VALUE, StatsType.NDV);
                        StatisticsDesc columnStatsDesc = new StatisticsDesc(columnCategory, columnGranularity, statsTypes);
                        SQLStatisticsTask sqlTask = new SQLStatisticsTask(jobId, Collections.singletonList(columnStatsDesc));
                        tasks.add(sqlTask);
                    }
                });
            } else {
                for (String columnName : columnNameList) {
                    StatsCategory columnCategory = getColStatsCategory(dbId, tblId, columnName);
                    StatsGranularity columnGranularity = getTblStatsGranularity(tblId);
                    List<StatsType> statsTypes = Arrays.asList(StatsType.MIN_VALUE, StatsType.MAX_VALUE, StatsType.NDV);
                    StatisticsDesc columnStatsDesc = new StatisticsDesc(columnCategory, columnGranularity, statsTypes);
                    SQLStatisticsTask sqlTask = new SQLStatisticsTask(jobId, Collections.singletonList(columnStatsDesc));
                    tasks.add(sqlTask);
                }
            }

            // step 4: generate num_nulls task
            for (String columnName : columnNameList) {
                StatsCategory columnCategory = getColStatsCategory(dbId, tblId, columnName);
                StatsGranularity columnGranularity = getTblStatsGranularity(tblId);
                List<StatsType> statsTypes = Collections.singletonList(StatsType.NUM_NULLS);
                StatisticsDesc columnStatsDesc = new StatisticsDesc(columnCategory, columnGranularity, statsTypes);
                SQLStatisticsTask sqlTask = new SQLStatisticsTask(jobId, Collections.singletonList(columnStatsDesc));
                tasks.add(sqlTask);
            }

            // step 5: generate [max_col_lens, avg_col_lens] task
            for (String columnName : columnNameList) {
                StatsCategory columnCategory = getColStatsCategory(dbId, tblId, columnName);
                StatsGranularity columnGranularity = getTblStatsGranularity(tblId);
                List<StatsType> statsTypes = Arrays.asList(StatsType.MAX_SIZE, StatsType.AVG_SIZE);
                Column column = tbl.getColumn(columnName);
                Type colType = column.getType();
                if (colType.isStringType()) {
                    StatisticsDesc columnStatsDesc = new StatisticsDesc(columnCategory, columnGranularity, statsTypes);
                    SQLStatisticsTask sampleSqlTask = new SampleSQLStatisticsTask(jobId, Collections.singletonList(columnStatsDesc));
                    tasks.add(sampleSqlTask);
                } else {
                    StatisticsDesc colStatsDesc = new StatisticsDesc(columnCategory, columnGranularity, statsTypes);
                    metaStatsTask.getStatsDescs().add(colStatsDesc);
                }
            }
            tasks.add(metaStatsTask);
        }
    }

    private StatsCategory getTblStatsCategory(long dbId, long tableId) {
        StatsCategory category = new StatsCategory();
        category.setCategory(StatsCategory.Category.TABLE);
        category.setDbId(dbId);
        category.setTableId(tableId);
        return category;
    }

    private StatsCategory getColStatsCategory(long dbId, long tableId, String columnName) {
        StatsCategory category = new StatsCategory();
        category.setDbId(dbId);
        category.setTableId(tableId);
        category.setCategory(StatsCategory.Category.COLUMN);
        category.setColumnName(columnName);
        return category;
    }

    private StatsGranularity getTblStatsGranularity(long tableId) {
        StatsGranularity statsGranularity = new StatsGranularity();
        statsGranularity.setTableId(tableId);
        statsGranularity.setGranularity(StatsGranularity.Granularity.TABLE);
        return statsGranularity;
    }

    private StatsGranularity getPartitionStatsGranularity(long tableId, long partitionId) {
        StatsGranularity granularity = new StatsGranularity();
        granularity.setTableId(tableId);
        granularity.setPartitionId(partitionId);
        granularity.setGranularity(StatsGranularity.Granularity.PARTITION);
        return granularity;
    }

    private StatsGranularity getTabletStatsGranularity(long tableId) {
        StatsGranularity granularity = new StatsGranularity();
        granularity.setTableId(tableId);
        granularity.setGranularity(StatsGranularity.Granularity.PARTITION);
        return granularity;
    }
}

