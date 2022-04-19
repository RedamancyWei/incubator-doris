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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;

import org.glassfish.jersey.internal.guava.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mockit.Expectations;
import mockit.Mocked;

public class StatisticsJobTest {

    private StatisticsJob statisticsJobUnderTest;

    @Before
    public void setUp() throws Exception {
        HashSet<Long> tblIds = Sets.newHashSet();
        tblIds.add(0L);
        tblIds.add(1L);
        this.statisticsJobUnderTest = new StatisticsJob(0L, tblIds, new HashMap<>());
    }

    @Test
    public void testRelatedTableId() {
        // Run the test
        final Set<Long> result = this.statisticsJobUnderTest.getTblIds();
        // Verify the results
        Assert.assertEquals(new HashSet<>(Arrays.asList(0L, 1L)), result);
    }

    @Test
    public void testGetShowInfo(@Mocked Catalog catalog, @Mocked Database database, @Mocked OlapTable table) throws Exception {
        // Setup
        Column col1 = new Column("col1", PrimitiveType.STRING);
        Column col2 = new Column("col2", PrimitiveType.INT);
        List<Column> columns = Arrays.asList(col1, col2);

        StatsCategoryDesc statsCategoryDesc = new StatsCategoryDesc();
        statsCategoryDesc.setDbId(0L);
        statsCategoryDesc.setTableId(0L);
        statsCategoryDesc.setCategory(StatsCategoryDesc.StatsCategory.COLUMN);
        statsCategoryDesc.setColumnName("col1");

        StatsGranularityDesc statsGranularityDesc = new StatsGranularityDesc();
        statsGranularityDesc.setTableId(0L);
        statsGranularityDesc.setGranularity(StatsGranularityDesc.StatsGranularity.TABLE);

        this.statisticsJobUnderTest.setTasks(Collections.singletonList(new StatisticsTask(0L, statsGranularityDesc, statsCategoryDesc, null) {
            @Override
            public StatisticsTaskResult call() throws Exception {
                return null;
            }
        }));

        new Expectations() {
            {
                catalog.getDbOrAnalysisException(this.anyLong);
                this.minTimes = 0;
                this.result = database;

                table.getBaseSchema();
                this.minTimes = 0;
                this.result = columns;

            }
        };

        // Run the test
        final List<Comparable> result = this.statisticsJobUnderTest.getShowInfo(0L);

        // Verify the results
        Assert.assertEquals(result.size(), 8);
        Assert.assertEquals(result.get(7), "PENDING");
    }
}
