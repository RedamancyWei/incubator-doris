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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.StatisticsJob.JobState;
import org.apache.doris.statistics.StatisticsTask.TaskState;
import org.apache.doris.statistics.util.Connection;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Schedule statistics task
 */
public class StatisticsTaskScheduler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticsTaskScheduler.class);
    /**
     * the maximum number of connections per database.
     * each database can only be connected to CAPACITY.
     */
    private static final int CAPACITY = 10;
    private final Map<String, ConnectionPool> dbToConnections = Maps.newHashMap();

    private final Queue<StatisticsTask> queue = Queues.newLinkedBlockingQueue();

    static class ConnectionPool {
        private int size = 0;
        private final String database;
        private final List<Connection> connections = Lists.newArrayList();

        public ConnectionPool(String database) {
            this.database = database;
            Connection connection = new Connection(database);
            connections.add(connection);
            size++;
        }

        public synchronized Connection getConnection(long timeoutSec) throws InterruptedException, TimeoutException {
            int clientNums = connections.size();
            if (clientNums > 0) {
                Connection connection = connections.get(clientNums - 1);
                connections.remove(clientNums - 1);
                return connection;
            } else {
                if (size < CAPACITY) {
                    Connection connection = new Connection(database);
                    size++;
                    return connection;
                } else {
                    long currentTime = System.currentTimeMillis();
                    while (true) {
                        TimeUnit.SECONDS.sleep(1);
                        if (System.currentTimeMillis() - currentTime > timeoutSec * 1000) {
                            throw new TimeoutException("Get client connection timeout.");
                        } else if (connections.size() > 0) {
                            return connections.get(connections.size() - 1);
                        }
                    }
                }
            }
        }

        public synchronized void close(Connection connection) {
            connections.add(connection);
        }
    }

    public StatisticsTaskScheduler() {
        super("Statistics task scheduler", 0);
    }

    public synchronized ConnectionPool getConnectionPool(String database) {
        ConnectionPool connectionPool;
        if (dbToConnections.containsKey(database)) {
            connectionPool = dbToConnections.get(database);
        } else {
            connectionPool = new ConnectionPool(database);
            dbToConnections.put(database, connectionPool);
        }
        return connectionPool;
    }

    @Override
    protected void runAfterCatalogReady() {
        // step1: task n concurrent tasks from the queue
        List<StatisticsTask> tasks = peek();

        if (!tasks.isEmpty()) {
            ThreadPoolExecutor executor = ThreadPoolManager.newDaemonCacheThreadPool(tasks.size(),
                    "statistic-pool", false);
            StatisticsJobManager jobManager = Catalog.getCurrentCatalog().getStatisticsJobManager();
            Map<Long, StatisticsJob> statisticsJobs = jobManager.getIdToStatisticsJob();
            Map<Long, List<Map<Long, Future<StatisticsTaskResult>>>> resultMap = Maps.newLinkedHashMap();

            for (StatisticsTask task : tasks) {
                queue.remove();
                long jobId = task.getJobId();

                if (checkJobIsValid(jobId)) {
                    // step2: execute task and save task result
                    Future<StatisticsTaskResult> future = executor.submit(task);
                    StatisticsJob statisticsJob = statisticsJobs.get(jobId);

                    if (updateTaskAndJobState(task, statisticsJob)) {
                        Map<Long, Future<StatisticsTaskResult>> taskInfo = Maps.newHashMap();
                        taskInfo.put(task.getId(), future);
                        List<Map<Long, Future<StatisticsTaskResult>>> jobInfo = resultMap
                                .getOrDefault(jobId, Lists.newArrayList());
                        jobInfo.add(taskInfo);
                        resultMap.put(jobId, jobInfo);
                    }
                }
            }

            // step3: handle task results
            handleTaskResult(resultMap);
        }
    }

    public void addTasks(List<StatisticsTask> statisticsTaskList) throws IllegalStateException {
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

    /**
     * Update task and job state
     *
     * @param task statistics task
     * @param job statistics job
     * @return true if update task and job state successfully.
     */
    private boolean updateTaskAndJobState(StatisticsTask task, StatisticsJob job) {
        try {
            // update task state
            task.updateTaskState(TaskState.RUNNING);
        } catch (DdlException e) {
            LOG.info("Update statistics task state failed, taskId: " + task.getId(), e);
        }

        try {
            // update job state
            if (task.getTaskState() != TaskState.RUNNING) {
                job.updateJobState(JobState.FAILED);
            } else {
                if (job.getJobState() == JobState.SCHEDULING) {
                    job.updateJobState(JobState.RUNNING);
                }
            }
        } catch (DdlException e) {
            LOG.info("Update statistics job state failed, jobId: " + job.getId(), e);
            return false;
        }
        return true;
    }

    private void handleTaskResult(Map<Long, List<Map<Long, Future<StatisticsTaskResult>>>> resultMap) {
        StatisticsManager statsManager = Catalog.getCurrentCatalog().getStatisticsManager();
        StatisticsJobManager jobManager = Catalog.getCurrentCatalog().getStatisticsJobManager();

        resultMap.forEach((jobId, taskMapList) -> {
            if (checkJobIsValid(jobId)) {
                StatisticsJob statisticsJob = jobManager.getIdToStatisticsJob().get(jobId);
                Map<String, String> properties = statisticsJob.getProperties();
                long timeout = Long.parseLong(properties.get(AnalyzeStmt.CBO_STATISTICS_TASK_TIMEOUT_SEC));

                // For tasks with tablet granularity,
                // we need aggregate calculations to get the results of the statistics,
                // so we need to put all the tasks together and handle the results together.
                List<StatisticsTaskResult> taskResults = Lists.newArrayList();

                for (Map<Long, Future<StatisticsTaskResult>> taskInfos : taskMapList) {
                    taskInfos.forEach((taskId, future) -> {
                        String errorMsg = "";

                        try {
                            StatisticsTaskResult taskResult = future.get(timeout, TimeUnit.SECONDS);
                            taskResults.add(taskResult);
                        } catch (TimeoutException | ExecutionException | InterruptedException
                                | CancellationException e) {
                            errorMsg = e.getMessage();
                            LOG.info("Failed to get statistics. jobId: {}, taskId: {}, e: {}", jobId, taskId, e);
                        }

                        try {
                            statisticsJob.updateJobInfoByTaskId(taskId, errorMsg);
                        } catch (DdlException e) {
                            LOG.info("Failed to update statistics job info. jobId: {}, e: {}", jobId, e);
                        }
                    });
                }

                try {
                    statsManager.updateStatistics(taskResults);
                } catch (AnalysisException e) {
                    LOG.info("Failed to update statistics. jobId: {}, e: {}", jobId, e);
                }
            }
        });
    }

    public boolean checkJobIsValid(Long jobId) {
        StatisticsJobManager jobManager = Catalog.getCurrentCatalog().getStatisticsJobManager();
        StatisticsJob statisticsJob = jobManager.getIdToStatisticsJob().get(jobId);
        if (statisticsJob == null) {
            return false;
        }
        JobState jobState = statisticsJob.getJobState();
        return jobState != JobState.CANCELLED && jobState != JobState.FAILED;
    }
}
