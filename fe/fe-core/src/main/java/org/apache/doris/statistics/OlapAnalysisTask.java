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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.text.StringSubstitutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Each task analyze one column.
 */
public class OlapAnalysisTask extends BaseAnalysisTask {

    /**
     * Analyze partition statistics.
     * The statistics are first collected in a partitioned manner,
     * and the non-partitioned table will also have a partition in doris with the same table name,
     * after collecting partition statistics, aggregate the statistics in columns.
     */
    private static final String ANALYZE_PARTITION_STATISTIC_TEMPLATE =  "INSERT INTO "
            + StatisticConstants.STATISTIC_TBL_FULL_NAME
            + "SELECT "
            + "    CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}', '-', ${partId}) AS id, "
            + "    ${catalogId} AS catalog_id, "
            + "    ${dbId} AS db_id, "
            + "    ${tblId} AS tbl_id, "
            + "    ${idxId} AS idx_id, "
            + "    '${colId}' AS col_id, "
            + "    ${partId} AS part_id, "
            + "    COUNT(1) AS row_count, "
            + "    NDV(`${colName}`) AS ndv, "
            + "    SUM(CASE WHEN `${colName}` IS NULL THEN 1 ELSE 0 END) AS null_count, "
            + "    MIN(`${colName}`) AS min, "
            + "    MAX(`${colName}`) AS max, "
            + "    ${dataSizeFunction} AS data_size, "
            + "    NOW() "
            + "FROM "
            + "    `${dbName}`.`${tblName}` PARTITION ${partName}";

    /**
     * Analyze column statistics.
     * The column statistics are aggregated from the partition statistics,
     * some statistics indicators cannot be aggregated and need to be queried in the original table.
     */
    private static final String ANALYZE_COLUMN_STATISTIC_TEMPLATE = "INSERT INTO "
            + StatisticConstants.STATISTIC_TBL_FULL_NAME
            + "SELECT "
            + "    id, catalog_id, db_id, tbl_id, idx_id, col_id, part_id, "
            + "    row_count, ndv, null_count, min, max, data_size, update_time "
            + "FROM "
            + "    (SELECT "
            + "        CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "        ${catalogId} AS catalog_id, ${dbId} AS db_id, "
            + "        ${tblId} AS tbl_id, ${idxId} AS idx_id, "
            + "        '${colId}' AS col_id, NULL AS part_id, "
            + "        SUM(count) AS row_count, "
            + "        SUM(null_count) AS null_count, "
            + "        MIN(CAST(min AS ${type})) AS min, "
            + "        MAX(CAST(max AS ${type})) AS max, "
            + "        SUM(data_size_in_bytes) AS data_size, "
            + "        NOW() AS update_time "
            + "     FROM "
            + "        ${internalDB}.${columnStatTbl}"
            + "     WHERE "
            + "        ${internalDB}.${columnStatTbl}.db_id = '${dbId}' "
            + "        AND ${internalDB}.${columnStatTbl}.tbl_id='${tblId}' "
            + "        AND ${internalDB}.${columnStatTbl}.col_id='${colId}' "
            + "        AND ${internalDB}.${columnStatTbl}.idx_id='${idxId}' "
            + "        AND ${internalDB}.${columnStatTbl}.part_id IS NOT NULL "
            + "    ) t1, "
            + "    (SELECT "
            + "        NDV(`${colName}`) AS ndv "
            + "    FROM "
            + "        `${dbName}`.`${tblName}`) t2";

    @VisibleForTesting
    public OlapAnalysisTask() {
        super();
    }

    public OlapAnalysisTask(AnalysisTaskScheduler analysisTaskScheduler, AnalysisTaskInfo info) {
        super(analysisTaskScheduler, info);
    }

    public void execute() throws Exception {
        Map<String, String> params = getBaseParams();
        List<String> partitionAnalysisSQLs = new ArrayList<>();
        try {
            tbl.readLock();
            Set<String> partNames = tbl.getPartitionNames();
            for (String partName : partNames) {
                Partition part = tbl.getPartition(partName);
                if (part == null) {
                    continue;
                }
                params.put("partId", String.valueOf(tbl.getPartition(partName).getId()));
                params.put("partName", String.valueOf(partName));
                StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
                partitionAnalysisSQLs.add(stringSubstitutor.replace(ANALYZE_PARTITION_STATISTIC_TEMPLATE));
            }
        } finally {
            tbl.readUnlock();
        }
        execSQLs(partitionAnalysisSQLs);
        params.remove("partId");
        params.put("type", col.getType().toString());
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(ANALYZE_COLUMN_STATISTIC_TEMPLATE);
        execSQL(sql);
        Env.getCurrentEnv().getStatisticsCache().refreshSync(tbl.getId(), -1, col.getName());
    }

    @VisibleForTesting
    public void execSQLs(List<String> partitionAnalysisSQLs) throws Exception {
        for (String sql : partitionAnalysisSQLs) {
            execSQL(sql);
        }
    }

    @VisibleForTesting
    public void execSQL(String sql) throws Exception {
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext()) {
            this.stmtExecutor = new StmtExecutor(r.connectContext, sql);
            this.stmtExecutor.execute();
        }
    }
}
