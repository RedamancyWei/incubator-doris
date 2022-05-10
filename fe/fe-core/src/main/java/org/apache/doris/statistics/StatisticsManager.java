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

import org.apache.doris.analysis.AlterColumnStatsStmt;
import org.apache.doris.analysis.AlterTableStatsStmt;
import org.apache.doris.analysis.ShowColumnStatsStmt;
import org.apache.doris.analysis.ShowTableStatsStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class StatisticsManager {
    private static final Logger LOG = LogManager.getLogger(StatisticsManager.class);

    private Statistics statistics;

    public StatisticsManager() {
        statistics = new Statistics();
    }

    public Statistics getStatistics() {
        return statistics;
    }

    /**
     * Alter table or partition stats. if partition name is not null, update partition stats.
     *
     * @param stmt alter table stats stmt
     * @throws AnalysisException if table or partition not exist
     */
    public void alterTableStatistics(AlterTableStatsStmt stmt) throws AnalysisException {
        Table table = validateTableName(stmt.getTableName());
        String partitionName = validatePartitionName(table, stmt.getPartitionName());
        Map<StatsType, String> statsTypeToValue = stmt.getStatsTypeToValue();

        for (Map.Entry<StatsType, String> entry : statsTypeToValue.entrySet()) {
            StatsType statsType = entry.getKey();
            String value = entry.getValue();
            if (Strings.isNullOrEmpty(partitionName)) {
                statistics.updateTableStats(table.getId(), statsType, value);
            } else {
                statistics.updatePartitionStats(table.getId(), partitionName, statsType, value);
            }
        }
    }

    /**
     * Alter column stats. if partition name is not null, update column of partition stats.
     *
     * @param stmt alter column stats stmt
     * @throws AnalysisException if table, column or partition not exist
     */
    public void alterColumnStatistics(AlterColumnStatsStmt stmt) throws AnalysisException {
        Table table = validateTableName(stmt.getTableName());
        String columnName = stmt.getColumnName();
        Column column = validateColumn(table, columnName);

        // match type and column value
        Type colType = column.getType();
        String partitionName = validatePartitionName(table, stmt.getPartitionName());
        Map<StatsType, String> statsTypeToValue = stmt.getStatsTypeToValue();

        for (Map.Entry<StatsType, String> entry : statsTypeToValue.entrySet()) {
            StatsType statsType = entry.getKey();
            String value = entry.getValue();
            if (Strings.isNullOrEmpty(partitionName)) {
                statistics.updateColumnStats(table.getId(), column.getName(), colType, statsType, value);
            } else {
                statistics.updateColumnStats(table.getId(), partitionName, column.getName(), colType, statsType, value);
            }
        }
    }

    /**
     * Update statistics. there are three types of statistics: column, table and column.
     *
     * @param taskResult statistics task result
     * @throws AnalysisException if column, table or partition not exist
     */
    public void updateStatistics(StatisticsTaskResult taskResult) throws AnalysisException {
        Map<StatsType, List<StatsCategory>> statsTypeToValue = taskResult.getStatsTypeToValue();

        for (Map.Entry<StatsType, List<StatsCategory>> entry : statsTypeToValue.entrySet()) {
            StatsType statsType = entry.getKey();
            List<StatsCategory> statsCategories = entry.getValue();

            for (StatsCategory category : statsCategories) {
                validateStatsCategory(category);
                long tblId = category.getTableId();
                String value = category.getStatsValue();
                String partitionName = category.getPartitionName();

                switch (category.getCategory()) {
                    case TABLE:
                        statistics.updateTableStats(tblId, statsType, value);
                        break;
                    case PARTITION:
                        statistics.updatePartitionStats(tblId, partitionName, statsType, value);
                        break;
                    case COLUMN:
                        updateColumnStats(statsType, category, tblId, value);
                        break;
                    default:
                        throw new AnalysisException("Unknown stats category: " + category.getCategory());
                }
            }
        }
    }

    private void updateColumnStats(StatsType statsType, StatsCategory category, long tblId, String value)
            throws AnalysisException {
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(category.getDbId());
        OlapTable tbl = (OlapTable) db.getTableOrAnalysisException(tblId);

        String colName = category.getColumnName();
        Column column = tbl.getColumn(colName);
        Type columnType = column.getType();
        String partitionName = category.getPartitionName();

        if (Strings.isNullOrEmpty(partitionName)) {
            statistics.updateColumnStats(tblId, colName, columnType, statsType, value);
        } else {
            statistics.updateColumnStats(tblId, partitionName, colName, columnType, statsType, value);
        }
    }

    /**
     * Get the statistics of a table. if specified partition name, get the statistics of the partition.
     *
     * @param stmt statement
     * @return partition or table statistics
     * @throws AnalysisException statistics not exist
     */
    public List<List<String>> showTableStatsList(ShowTableStatsStmt stmt) throws AnalysisException {
        String dbName = stmt.getDbName();
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(dbName);
        String tableName = stmt.getTableName();

        List<List<String>> result = Lists.newArrayList();

        if (tableName != null) {
            Table table = db.getTableOrAnalysisException(tableName);
            // check priv
            if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName, tableName,
                    PrivPredicate.SHOW)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW CREATE TABLE",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(),
                        dbName + ": " + tableName);
            }

            // check partition
            String partitionName = validatePartitionName(table, stmt.getPartitionName());

            // get stats
            if (Strings.isNullOrEmpty(partitionName)) {
                result.add(showTableStats(table));
            } else {
                result.add(showTableStats(table, partitionName));
            }
        } else {
            for (Table table : db.getTables()) {
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName, table.getName(),
                        PrivPredicate.SHOW)) {
                    continue;
                }
                try {
                    result.add(showTableStats(table));
                } catch (AnalysisException e) {
                    // ignore no stats table
                }
            }
        }
        return result;
    }

    /**
     * Get the column statistics of a table. if specified partition name, get the column statistics of the partition.
     *
     * @param stmt statement
     * @return column statistics for  a partition or table
     * @throws AnalysisException statistics not exist
     */
    public List<List<String>> showColumnStatsList(ShowColumnStatsStmt stmt) throws AnalysisException {
        TableName tableName = stmt.getTableName();

        // check meta
        Table table = validateTableName(tableName);

        // check priv
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tableName.getDb(),
                tableName.getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW CREATE TABLE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tableName.getDb() + ": " + tableName.getTbl());
        }

        // check partition
        String partitionName = validatePartitionName(table, stmt.getPartitionName());

        // get stats
        if (Strings.isNullOrEmpty(partitionName)) {
            return showColumnStats(table.getId());
        } else {
            return showColumnStats(table.getId(), partitionName);
        }
    }

    private List<String> showTableStats(Table table) throws AnalysisException {
        TableStats tableStats = statistics.getTableStats(table.getId());
        if (tableStats == null) {
            throw new AnalysisException("There is no statistics in this table:" + table.getName());
        }
        List<String> row = Lists.newArrayList();
        row.add(table.getName());
        row.addAll(tableStats.getShowInfo());
        return row;
    }

    private List<String> showTableStats(Table table, String partitionName) throws AnalysisException {
        Map<String, PartitionStats> partitionStats = statistics.getPartitionStats(table.getId(), partitionName);
        PartitionStats partitionStat = partitionStats.get(partitionName);
        if (partitionStat == null) {
            throw new AnalysisException("There is no statistics in this partition:" + partitionName);
        }
        List<String> row = Lists.newArrayList();
        row.add(table.getName());
        row.addAll(partitionStat.getShowInfo());
        return row;
    }

    private List<List<String>> showColumnStats(long tableId) throws AnalysisException {
        List<List<String>> result = Lists.newArrayList();
        Map<String, ColumnStats> columnStats = statistics.getColumnStats(tableId);
        columnStats.forEach((key, stats) -> {
            List<String> row = Lists.newArrayList();
            row.add(key);
            row.addAll(stats.getShowInfo());
            result.add(row);
        });
        return result;
    }

    private List<List<String>> showColumnStats(long tableId, String partitionName) throws AnalysisException {
        List<List<String>> result = Lists.newArrayList();
        Map<String, ColumnStats> columnStats = statistics.getColumnStats(tableId, partitionName);
        columnStats.forEach((key, stats) -> {
            List<String> row = Lists.newArrayList();
            row.add(key);
            row.addAll(stats.getShowInfo());
            result.add(row);
        });
        return result;
    }

    private Table validateTableName(TableName dbTableName) throws AnalysisException {
        String dbName = dbTableName.getDb();
        String tableName = dbTableName.getTbl();
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(dbName);
        return db.getTableOrAnalysisException(tableName);
    }

    private String validatePartitionName(Table table, String partitionName) throws AnalysisException {
        if (!table.isPartitioned() && !Strings.isNullOrEmpty(partitionName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_PARTITION_CLAUSE_ON_NONPARTITIONED, partitionName);
            throw new AnalysisException("Table " + table.getName() + " is not partitioned");
        }

        if (table.isPartitioned() && table.getPartition(partitionName) == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_PARTITION, partitionName);
        }

        return partitionName;
    }

    private Column validateColumn(Table table, String columnName) throws AnalysisException {
        Column column = table.getColumn(columnName);
        if (column == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, table.getName());
        }
        return column;
    }

    private void validateStatsCategory(StatsCategory category) throws AnalysisException {
        long dbId = category.getDbId();
        long tblId = category.getTableId();
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(dbId);
        Table table = db.getTableOrAnalysisException(tblId);
        validatePartitionName(table, category.getPartitionName());
        validateColumn(table, category.getColumnName());
    }
}
