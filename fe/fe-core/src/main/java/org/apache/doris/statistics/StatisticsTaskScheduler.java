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

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.StatisticsJob.JobState;
import org.apache.doris.statistics.StatsCategoryDesc.StatsCategory;

import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Future;

import com.clearspring.analytics.util.Lists;

/*
Schedule statistics task
 */
public class StatisticsTaskScheduler extends MasterDaemon {
    private final static Logger LOG = LogManager.getLogger(StatisticsTaskScheduler.class);

    private final Queue<StatisticsTask> queue = Queues.newLinkedBlockingQueue();

    public StatisticsTaskScheduler() {
        super("Statistics task scheduler", 0);
    }

    @Override
    protected void runAfterCatalogReady() {
        // step1: task n concurrent tasks from the queue
        List<StatisticsTask> tasks = peek();

        if (!tasks.isEmpty()) {
            ThreadPoolExecutor executor = ThreadPoolManager.newDaemonCacheThreadPool(tasks.size(),
                    "statistic-pool", true);
            StatisticsJobManager jobManager = Catalog.getCurrentCatalog().getStatisticsJobManager();
            Map<Long, StatisticsJob> statisticsJobs = jobManager.getIdToStatisticsJob();

            long jobId = -1;
            int taskSize = 0;
            Map<Long, Future<StatisticsTaskResult>> taskMap = Maps.newLinkedHashMap();

            for (StatisticsTask task : tasks) {
                if (taskSize > 0 && jobId != task.getJobId()){
                    // step3: update job and statistics
                    handleTaskResult(jobId, taskMap);
                    // step4: remove task from queue
                    remove(taskSize);
                    taskMap.clear();
                    taskSize = 0;
                }
                // step2: to execute tasks
                Future<StatisticsTaskResult> future = executor.submit(task);
                long taskId = task.getId();
                taskMap.put(taskId, future);
                StatisticsJob statisticsJob = statisticsJobs.get(jobId);
                if (statisticsJob.getJobState() == JobState.SCHEDULING) {
                    statisticsJob.setJobState(JobState.RUNNING);
                }
                taskSize ++;
                jobId = task.getJobId();
            }
            if (taskSize > 0){
                handleTaskResult(jobId, taskMap);
                remove(taskSize);
            }
        }
    }

    public void addTasks(List<StatisticsTask> statisticsTaskList) {
        queue.addAll(statisticsTaskList);
    }

    private List<StatisticsTask> peek() {
        List<StatisticsTask> tasks = Lists.newArrayList();
        int i = Config.cbo_concurrency_statistics_task_num;
        while (i > 0) {
            StatisticsTask task = queue.peek();
            if (task == null) {
                break;
            }
            tasks.add(task);
            i--;
        }
        return tasks;
    }

    private void remove(int size) {
        for (int i = 0; i < size; i++) {
            queue.poll();
        }
    }

    private void handleTaskResult(Long jobId, Map<Long, Future<StatisticsTaskResult>> taskMap) {
        StatisticsManager statsManager = Catalog.getCurrentCatalog().getStatisticsManager();
        StatisticsJobManager jobManager = Catalog.getCurrentCatalog().getStatisticsJobManager();

        Set<Map.Entry<Long, Future<StatisticsTaskResult>>> entries = taskMap.entrySet();
        for (Map.Entry<Long, Future<StatisticsTaskResult>> entry : entries) {
            Exception exception = null;
            long taskId = entry.getKey();
            Future<StatisticsTaskResult> future = entry.getValue();
            try {
                StatisticsTaskResult taskResult = future.get(30, TimeUnit.MINUTES);
                StatsCategoryDesc categoryDesc = taskResult.getCategoryDesc();
                StatsCategory category = categoryDesc.getCategory();
                if (category == StatsCategory.TABLE) {
                    // update table statistics
                    statsManager.alterTableStatistics(taskResult);
                } else if (category == StatsCategory.COLUMN) {
                    // update column statistics
                    statsManager.alterColumnStatistics(taskResult);
                }
            } catch (InterruptedException | ExecutionException | TimeoutException | AnalysisException e) {
                exception = e;
                LOG.warn("Failed to execute this turn of statistics tasks", exception);
            }
            // update the job info
            jobManager.alterStatisticsJobInfo(jobId, taskId, exception);
        }
    }
}
