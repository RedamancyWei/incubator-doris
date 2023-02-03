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

import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SelectListItem;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Preconditions;

import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Analysis for the materialized view, only gets constructed when the AnalyzeStmt is not set which
 * columns to be analyzed.
 * TODO: Supports multi-table mv
 */
public class MVAnalysisTask extends BaseAnalysisTask {

    private static final String ANALYZE_MV_PARTITION_STATISTIC_TEMPLATE = "INSERT INTO "
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
            + "    (${sql}) mv";

    private static final String ANALYZE_MV_COL =  "INSERT INTO "
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
            + "        (${sql}) mv) t2";

    private MaterializedIndexMeta meta;

    private SelectStmt selectStmt;

    private OlapTable olapTable;

    public MVAnalysisTask(AnalysisTaskScheduler analysisTaskScheduler, AnalysisTaskInfo info) {
        super(analysisTaskScheduler, info);
        init();
    }

    private void init() {
        olapTable = (OlapTable) tbl;
        meta = olapTable.getIndexMetaByIndexId(info.indexId);
        Preconditions.checkState(meta != null);
        String mvDef = meta.getDefineStmt().originStmt;
        SqlScanner input =
                new SqlScanner(new StringReader(mvDef), 0L);
        SqlParser parser = new SqlParser(input);
        CreateMaterializedViewStmt cmv = null;
        try {
            cmv = (CreateMaterializedViewStmt) SqlParserUtils.getStmt(parser, 0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        selectStmt = cmv.getSelectStmt();
        selectStmt.getTableRefs().get(0).getName().setDb(db.getFullName());
    }

    @Override
    public void execute() throws Exception {
        for (Column column : meta.getSchema()) {
            SelectStmt selectOne = (SelectStmt) selectStmt.clone();
            TableRef tableRef = selectOne.getTableRefs().get(0);
            SelectListItem selectItem = selectOne.getSelectList().getItems()
                    .stream()
                    .filter(i -> isCorrespondingToColumn(i, column))
                    .findFirst()
                    .get();
            selectItem.setAlias(column.getName());
            Map<String, String> params = new HashMap<>();
            for (Partition part : olapTable.getAllPartitions()) {
                String partName = part.getName();
                PartitionNames partitionName = new PartitionNames(false, Arrays.asList(partName));
                tableRef.setPartitionNames(partitionName);
                String sql = selectOne.toSql();
                params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
                params.put("columnStatTbl", StatisticConstants.STATISTIC_TBL_NAME);
                params.put("catalogId", String.valueOf(catalog.getId()));
                params.put("dbId", String.valueOf(db.getId()));
                params.put("tblId", String.valueOf(tbl.getId()));
                params.put("idxId", String.valueOf(meta.getIndexId()));
                String colName = column.getName();
                params.put("colId", colName);
                long partId = part.getId();
                params.put("partId", String.valueOf(partId));
                params.put("dataSizeFunction", getDataSizeFunction(column));
                params.put("dbName", info.dbName);
                params.put("colName", colName);
                params.put("tblName", String.valueOf(info.tblName));
                params.put("sql", sql);
                StatisticsUtil.execUpdate(ANALYZE_MV_PARTITION_STATISTIC_TEMPLATE, params);
            }
            params.remove("partId");
            params.put("type", column.getType().toString());
            StatisticsUtil.execUpdate(ANALYZE_MV_COL, params);
            Env.getCurrentEnv().getStatisticsCache()
                    .refreshSync(meta.getIndexId(), meta.getIndexId(), column.getName());
        }
    }

    //  Based on the fact that materialized view create statement's select expr only contains basic SlotRef and
    //  AggregateFunction.
    private boolean isCorrespondingToColumn(SelectListItem item, Column column) {
        Expr expr = item.getExpr();
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            return slotRef.getColumnName().equalsIgnoreCase(column.getName());
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr func = (FunctionCallExpr) expr;
            SlotRef slotRef = (SlotRef) func.getChild(0);
            return slotRef.getColumnName().equalsIgnoreCase(column.getName());
        }
        return false;
    }
}
