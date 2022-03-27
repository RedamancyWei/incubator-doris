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

import com.clearspring.analytics.util.Lists;
import java.util.Date;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.catalog.Catalog;

import java.util.List;
import java.util.concurrent.Callable;
import org.apache.doris.common.DdlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The StatisticsTask belongs to one StatisticsJob.
 * A job may be split into multiple tasks but a task can only belong to one job.
 * @granularityDesc, @categoryDesc, @statsTypeList
 * These three attributes indicate which statistics this task is responsible for collecting.
 * In general, a task will collect more than one @StatsType at the same time
 * while all of types belong to the same @granularityDesc and @categoryDesc.
 * For example: the task is responsible for collecting min, max, ndv of t1.c1 in partition p1.
 * @granularityDesc: StatsGranularity=partition
 */
public class StatisticsTask implements Callable<StatisticsTaskResult> {
    protected static final Logger LOG = LogManager.getLogger(StatisticsTask.class);

    public enum TaskState {
        CREATED,
        RUNNING,
        FINISHED,
        FAILED
    }

    protected long id;
    protected long jobId;
    protected StatsGranularityDesc granularityDesc;
    protected StatsCategoryDesc categoryDesc;
    protected List<StatsType> statsTypeList;
    protected TaskState taskState;
    protected final Date createTime;
    protected Date scheduleTime;
    protected Date finishTime;

    public StatisticsTask(long jobId, StatsGranularityDesc granularityDesc,
                          StatsCategoryDesc categoryDesc, List<StatsType> statsTypeList) {
        this.id = Catalog.getCurrentCatalog().getNextId();
        this.jobId = jobId;
        this.granularityDesc = granularityDesc;
        this.categoryDesc = categoryDesc;
        this.statsTypeList = statsTypeList;
        this.taskState = TaskState.CREATED;
        this.createTime = new Date(System.currentTimeMillis());
    }

    public long getId() {
        return id;
    }

    public long getJobId() {
        return jobId;
    }

    public StatsGranularityDesc getGranularityDesc() {
        return granularityDesc;
    }

    public StatsCategoryDesc getCategoryDesc() {
        return categoryDesc;
    }

    public List<StatsType> getStatsTypeList() {
        return statsTypeList;
    }

    public TaskState getTaskState() {
        return taskState;
    }

    public void setTaskState(TaskState taskState) {
        this.taskState = taskState;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public Date getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(Date scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public Date getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(Date finishTime) {
        this.finishTime = finishTime;
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        LOG.warn("execute invalid statistics task.");
        return null;
    }

    public List<String> getShowInfo() throws DdlException {
        List<String> result = Lists.newArrayList();
        result.add(Long.toString(jobId));
        result.add(Long.toString(id));
        result.add(taskState.toString());
        result.add(createTime.toString());
        result.add(scheduleTime.toString());
        result.add(finishTime.toString());

        List<String> statsName = Lists.newArrayList();
        for (StatsType statsType : statsTypeList) {
            statsName.add(statsType.getValue());
        }
        result.add(StringUtils.join(statsName.toArray(), ","));

        StatsGranularityDesc.StatsGranularity granularity = granularityDesc.getGranularity();
        result.add(granularity.toString());

        String columnName = categoryDesc.getColumnName();
        result.add(columnName);

        return result;
    }
}
