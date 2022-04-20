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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import mockit.Expectations;
import mockit.Mocked;

import static org.junit.Assert.assertEquals;

public class StatisticsJobManagerTest {

    private StatisticsJobManager statisticsJobManagerUnderTest;

    @Before
    public void setUp() throws Exception {
        this.statisticsJobManagerUnderTest = new StatisticsJobManager();
    }

    @Test
    public void testCreateStatisticsJob(@Mocked AnalyzeStmt analyzeStmt) {
        // Setup
        Database database = new Database(1L, "db");
        Catalog catalog = Catalog.getCurrentCatalog();
        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(1L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        new Expectations() {
            {
                analyzeStmt.getDbId();
                this.minTimes = 0;
                this.result = 1L;
            }
        };

        // Run the test and verify the results
        try {
            this.statisticsJobManagerUnderTest.createStatisticsJob(analyzeStmt);
        } catch (UserException e) {
            Assert.fail("UserException throws." + e);
        }
    }

    @Test(expected = UserException.class)
    public void testCreateStatisticsJob_throwUserException(@Mocked AnalyzeStmt analyzeStmt) throws UserException {
        // Setup
        Database database = new Database(1L, "db");
        Catalog catalog = Catalog.getCurrentCatalog();
        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(1L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        new Expectations() {
            {
                analyzeStmt.getDbId();
                this.minTimes = 0;
                this.result = 1L;

                analyzeStmt.getTblIds();
                this.minTimes = 0;
                this.result = Sets.newHashSet(1L);
            }
        };

        try {
            // Run the test and verify same table has two unfinished statistics job
            this.statisticsJobManagerUnderTest.createStatisticsJob(analyzeStmt);
            this.statisticsJobManagerUnderTest.createStatisticsJob(analyzeStmt);
        } catch (UserException e) {
            throw new UserException(e);
        }
    }

    @Test
    public void testAlterStatisticsJobInfo(@Mocked AnalyzeStmt analyzeStmt) {
        // Setup
        Database database = new Database(1L, "db");
        Catalog catalog = Catalog.getCurrentCatalog();
        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(1L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        new Expectations() {
            {
                analyzeStmt.getDbId();
                this.minTimes = 0;
                this.result = 1L;
            }
        };

        // Run the test and verify the results
        try {
            this.statisticsJobManagerUnderTest.createStatisticsJob(analyzeStmt);
            Map<Long, StatisticsJob> idToStatisticsJob = this.statisticsJobManagerUnderTest.getIdToStatisticsJob();
            for (Map.Entry<Long, StatisticsJob> entry : idToStatisticsJob.entrySet()) {
                Long jobId = entry.getKey();
                StatisticsJob statisticsJob = entry.getValue();
                statisticsJob.setTasks(Collections.singletonList(
                        new StatisticsTask(jobId, null, null, null) {
                            @Override
                            public StatisticsTaskResult call() throws Exception {
                                return null;
                            }
                        }));
                long taskId = statisticsJob.getTasks().get(0).getId();
                this.statisticsJobManagerUnderTest.alterStatisticsJobInfo(jobId, taskId, null);
                this.statisticsJobManagerUnderTest.alterStatisticsJobInfo(jobId, taskId, "error");
                break;
            }
        } catch (UserException e) {
            Assert.fail("UserException throws." + e);
        }
    }

    @Test
    public void testGetAnalyzeJobInfos(@Mocked ShowAnalyzeStmt showStmt) throws Exception {
        // Setup
        Database database = new Database(1L, "db");
        Catalog catalog = Catalog.getCurrentCatalog();
        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(1L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        statisticsJobManagerUnderTest.getIdToStatisticsJob()
                .put(1L, new StatisticsJob(1L, Sets.newHashSet(), null));

        new Expectations() {
            {
                showStmt.getJobIds();
                this.minTimes = 0;
                this.result = Lists.newArrayList(1L);

                showStmt.getLimit();
                this.minTimes = 0;
                this.result = 1;
            }
        };

        // Run the test
        final List<List<String>> result = statisticsJobManagerUnderTest.getAnalyzeJobInfos(showStmt);

        // Verify the results
        assertEquals(1, result.size());
    }

    @Test(expected = AnalysisException.class)
    public void testGetAnalyzeJobInfos_ThrowsAnalysisException(@Mocked ShowAnalyzeStmt showStmt) throws Exception {
        // Setup
        Database database = new Database(1L, "db");
        Catalog catalog = Catalog.getCurrentCatalog();
        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(1L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        statisticsJobManagerUnderTest.getIdToStatisticsJob()
                .put(1L, new StatisticsJob(1L, Sets.newHashSet(), null));

        new Expectations() {
            {
                showStmt.getJobIds();
                this.minTimes = 0;
                this.result = Lists.newArrayList(2L);

                showStmt.getLimit();
                this.minTimes = 0;
                this.result = 1;
            }
        };

        // Run the test
        try {
            statisticsJobManagerUnderTest.getAnalyzeJobInfos(showStmt);
        } catch (AnalysisException e) {
            // Verify the results
            assertEquals("errCode = 2, detailMessage = No such job id: 2", e.getMessage());
            throw e;
        }
    }
}
