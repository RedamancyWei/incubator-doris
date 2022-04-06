// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.
//
// package org.apache.doris.statistics;
//
// import org.apache.doris.analysis.AnalyzeStmt;
// import org.apache.doris.analysis.Analyzer;
// import org.apache.doris.analysis.UserIdentity;
// import org.apache.doris.catalog.Catalog;
// import org.apache.doris.catalog.Column;
// import org.apache.doris.catalog.Database;
// import org.apache.doris.catalog.HashDistributionInfo;
// import org.apache.doris.catalog.KeysType;
// import org.apache.doris.catalog.OlapTable;
// import org.apache.doris.catalog.PartitionInfo;
// import org.apache.doris.catalog.PrimitiveType;
// import org.apache.doris.common.UserException;
// import org.apache.doris.common.jmockit.Deencapsulation;
// import org.apache.doris.mysql.privilege.PaloAuth;
// import org.apache.doris.mysql.privilege.PrivPredicate;
//
// import org.junit.Assert;
// import org.junit.Before;
// import org.junit.Test;
//
// import java.util.Arrays;
// import java.util.Collections;
// import java.util.Map;
// import java.util.concurrent.ConcurrentHashMap;
//
// import mockit.Expectations;
// import mockit.Mocked;
//
// public class StatisticsJobManagerTest {
//
//     private StatisticsJobManager statisticsJobManagerUnderTest;
//
//     @Before
//     public void setUp() throws Exception {
//         statisticsJobManagerUnderTest = new StatisticsJobManager();
//     }
//
//     @Test
//     public void testCreateStatisticsJob(@Mocked AnalyzeStmt analyzeStmt,
//                                         @Mocked PaloAuth auth,
//                                         @Mocked Analyzer analyzer) {
//         // Setup
//         Column col1 = new Column("col1", PrimitiveType.STRING);
//         Column col2 = new Column("col2", PrimitiveType.INT);
//         OlapTable table = new OlapTable(1L, "tbl", Arrays.asList(col1, col2), KeysType.AGG_KEYS,
//                 new PartitionInfo(), new HashDistributionInfo());
//         Database database = new Database(1L, "db");
//         database.createTable(table);
//
//         Catalog catalog = Catalog.getCurrentCatalog();
//         Deencapsulation.setField(catalog, "statisticsJobScheduler", new StatisticsJobScheduler());
//
//         ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
//         fullNameToDb.put("cluster:db", database);
//         Deencapsulation.setField(catalog, "fullNameToDb", fullNameToDb);
//
//         ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
//         idToDb.put(1L, database);
//         Deencapsulation.setField(catalog, "idToDb", idToDb);
//
//         UserIdentity userIdentity = new UserIdentity("root", "host", false);
//
//         new Expectations() {
//             {
//                 analyzeStmt.getDbName();
//                 this.minTimes = 0;
//                 this.result = "cluster:db";
//
//                 analyzeStmt.getTblName();
//                 this.minTimes = 0;
//                 this.result = "tbl";
//
//                 analyzeStmt.getClusterName();
//                 this.minTimes = 0;
//                 this.result = "cluster";
//
//                 analyzeStmt.getAnalyzer();
//                 this.minTimes = 0;
//                 this.result = analyzer;
//
//                 analyzeStmt.getColumnNames();
//                 this.minTimes = 0;
//                 this.result = Arrays.asList("col1", "col2");
//
//                 analyzeStmt.getUserInfo();
//                 this.minTimes = 0;
//                 this.result = userIdentity;
//
//                 auth.checkDbPriv(userIdentity, this.anyString, PrivPredicate.SELECT);
//                 this.minTimes = 0;
//                 this.result = true;
//
//                 auth.checkTblPriv(userIdentity, this.anyString, this.anyString, PrivPredicate.SELECT);
//                 this.minTimes = 0;
//                 this.result = true;
//             }
//         };
//
//         // Run the test and verify the results
//         try {
//             statisticsJobManagerUnderTest.createStatisticsJob(analyzeStmt);
//         } catch (UserException e) {
//             Assert.fail("UserException throws.");
//         }
//     }
//
//     @Test(expected = UserException.class)
//     public void testCreateStatisticsJob_throwUserException(@Mocked AnalyzeStmt analyzeStmt,
//                                          @Mocked PaloAuth auth,
//                                          @Mocked Analyzer analyzer) throws UserException {
//         // Setup
//         Column col1 = new Column("col1", PrimitiveType.STRING);
//         Column col2 = new Column("col2", PrimitiveType.INT);
//         OlapTable table = new OlapTable(1L, "tbl", Arrays.asList(col1, col2), KeysType.AGG_KEYS,
//                 new PartitionInfo(), new HashDistributionInfo());
//         Database database = new Database(1L, "db");
//         database.createTable(table);
//
//         Catalog catalog = Catalog.getCurrentCatalog();
//         Deencapsulation.setField(catalog, "statisticsJobScheduler", new StatisticsJobScheduler());
//
//         ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
//         fullNameToDb.put("cluster:db", database);
//         Deencapsulation.setField(catalog, "fullNameToDb", fullNameToDb);
//
//         ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
//         idToDb.put(1L, database);
//         Deencapsulation.setField(catalog, "idToDb", idToDb);
//
//         UserIdentity userIdentity = new UserIdentity("root", "host", false);
//
//         new Expectations() {
//             {
//                 analyzeStmt.getDbName();
//                 this.minTimes = 0;
//                 this.result = "cluster:db";
//
//                 analyzeStmt.getTblName();
//                 this.minTimes = 0;
//                 this.result = "tbl";
//
//                 analyzeStmt.getClusterName();
//                 this.minTimes = 0;
//                 this.result = "cluster";
//
//                 analyzeStmt.getAnalyzer();
//                 this.minTimes = 0;
//                 this.result = analyzer;
//
//                 analyzeStmt.getColumnNames();
//                 this.minTimes = 0;
//                 this.result = Arrays.asList("col1", "col2");
//
//                 analyzeStmt.getUserInfo();
//                 this.minTimes = 0;
//                 this.result = userIdentity;
//
//                 auth.checkDbPriv(userIdentity, this.anyString, PrivPredicate.SELECT);
//                 this.minTimes = 0;
//                 this.result = true;
//
//                 auth.checkTblPriv(userIdentity, this.anyString, this.anyString, PrivPredicate.SELECT);
//                 this.minTimes = 0;
//                 this.result = true;
//             }
//         };
//
//         // Run the test and verify same table has two unfinished statistics job
//         statisticsJobManagerUnderTest.createStatisticsJob(analyzeStmt);
//         statisticsJobManagerUnderTest.createStatisticsJob(analyzeStmt);
//     }
//
//     @Test
//     public void testAlterStatisticsJobInfo(@Mocked AnalyzeStmt analyzeStmt,
//                                            @Mocked PaloAuth auth,
//                                            @Mocked Analyzer analyzer) {
//         // Setup
//         Column col1 = new Column("col1", PrimitiveType.STRING);
//         Column col2 = new Column("col2", PrimitiveType.INT);
//         OlapTable table = new OlapTable(1L, "tbl", Arrays.asList(col1, col2), KeysType.AGG_KEYS,
//                 new PartitionInfo(), new HashDistributionInfo());
//         Database database = new Database(1L, "db");
//         database.createTable(table);
//
//         Catalog catalog = Catalog.getCurrentCatalog();
//         Deencapsulation.setField(catalog, "statisticsJobScheduler", new StatisticsJobScheduler());
//
//         ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
//         fullNameToDb.put("cluster:db", database);
//         Deencapsulation.setField(catalog, "fullNameToDb", fullNameToDb);
//
//         ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
//         idToDb.put(1L, database);
//         Deencapsulation.setField(catalog, "idToDb", idToDb);
//
//         UserIdentity userIdentity = new UserIdentity("root", "host", false);
//
//         new Expectations() {
//             {
//                 analyzeStmt.getDbName();
//                 this.minTimes = 0;
//                 this.result = "cluster:db";
//
//                 analyzeStmt.getTblName();
//                 this.minTimes = 0;
//                 this.result = "tbl";
//
//                 analyzeStmt.getClusterName();
//                 this.minTimes = 0;
//                 this.result = "cluster";
//
//                 analyzeStmt.getAnalyzer();
//                 this.minTimes = 0;
//                 this.result = analyzer;
//
//                 analyzeStmt.getColumnNames();
//                 this.minTimes = 0;
//                 this.result = Arrays.asList("col1", "col2");
//
//                 analyzeStmt.getUserInfo();
//                 this.minTimes = 0;
//                 this.result = userIdentity;
//
//                 auth.checkDbPriv(userIdentity, this.anyString, PrivPredicate.SELECT);
//                 this.minTimes = 0;
//                 this.result = true;
//
//                 auth.checkTblPriv(userIdentity, this.anyString, this.anyString, PrivPredicate.SELECT);
//                 this.minTimes = 0;
//                 this.result = true;
//             }
//         };
//
//         // Run the test and verify the results
//         try {
//             statisticsJobManagerUnderTest.createStatisticsJob(analyzeStmt);
//             Map<Long, StatisticsJob> idToStatisticsJob = statisticsJobManagerUnderTest.getIdToStatisticsJob();
//             for (Map.Entry<Long, StatisticsJob> entry : idToStatisticsJob.entrySet()) {
//                 Long jobId = entry.getKey();
//                 StatisticsJob statisticsJob = entry.getValue();
//                 statisticsJob.setTasks(Collections.singletonList(new StatisticsTask(jobId, null, null, null)));
//                 long taskId = statisticsJob.getTasks().get(0).getId();
//                 statisticsJobManagerUnderTest.alterStatisticsJobInfo(jobId,taskId, null);
//                 statisticsJobManagerUnderTest.alterStatisticsJobInfo(jobId,taskId, new Exception("test"));
//                 break;
//             }
//         } catch (UserException e) {
//             Assert.fail("UserException throws.");
//         }
//     }
//
// }
