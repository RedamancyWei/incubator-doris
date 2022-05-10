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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Maps;

import java.util.Map;

import javax.annotation.Nullable;

/**
 * There are the statistics of all of tables.
 * The @Statistics are mainly used to provide input for the Optimizer's cost model.
 *
 * @idToTableStats: <@Long tableId, @TableStats tableStats>
 * Each table will have corresponding @TableStats.
 * Those @TableStats are recorded in @idToTableStats form of MAP.
 * This facilitates the optimizer to quickly find the corresponding
 * @TableStats based on the table id.
 */
public class Statistics {

    private final Map<Long, TableStats> idToTableStats = Maps.newConcurrentMap();

    // public void updateTableStats(long tableId, Map<StatsType, String> statsTypeToValue)
    //         throws AnalysisException {
    //     TableStats tableStats = idToTableStats.get(tableId);
    //     if (tableStats == null) {
    //         tableStats = new TableStats();
    //         idToTableStats.put(tableId, tableStats);
    //     }
    //     tableStats.updateTableStats(statsTypeToValue);
    // }

    public void updateTableStats(long tableId, StatsType statsType, String value) throws AnalysisException {
        TableStats tableStats = getTableStats(tableId);
        tableStats.updateTableStats(statsType, value);
    }

    public void updatePartitionStats(long tableId, long partitionId, StatsType statsType, String value)
        throws AnalysisException {
        PartitionStats partitionStat = getPartitionStats(tableId, partitionId);
        partitionStat.updatePartitionStats(statsType, value);
    }

    // public void updateColumnStats(long tableId, long partitionId, String columnName, Type columnType,
    //                               Map<StatsType, String> statsTypeToValue)
    //         throws AnalysisException {
    //     PartitionStats partitionStat = getPartitionStats(tableId, partitionId);
    //     partitionStat.updateColumnStats(columnName, columnType, statsTypeToValue);
    // }

    /**
     * Update the column stats of the partition.
     * For partitionId if null means the table is not partitioned, use the table id instead.
     * @see org.apache.doris.statistics.PartitionStats
     *
     * @param tableId table id
     * @param partitionId partition id
     * @param columnName column name
     * @param columnType column type
     * @param statsType stats type
     * @param value stats value
     * @throws AnalysisException
     */
    public void updateColumnStats(long tableId,
                                  @Nullable Long partitionId,
                                  String columnName,
                                  Type columnType,
                                  StatsType statsType,
                                  String value) throws AnalysisException {
        if (partitionId == null) {
            // TODO(wzt): support for updating column statistics on non-partitioned
            throw new AnalysisException("Unsupported non-partitioned table: " + tableId);
        }
        PartitionStats partitionStat = getPartitionStats(tableId, partitionId);
        partitionStat.updateColumnStats(columnName, columnType, statsType, value);
    }

    public TableStats getTableStats(long tableId) {
        TableStats tableStats = idToTableStats.get(tableId);
        if (tableStats == null) {
            tableStats = new TableStats();
            idToTableStats.put(tableId, tableStats);
        }
        return tableStats;
    }

    private PartitionStats getPartitionStats(long tableId, long partitionId) {
        TableStats tableStats = getTableStats(tableId);
        Map<Long, PartitionStats> partitionStats = tableStats.getIdToPartitionStats();
        PartitionStats partitionStat = partitionStats.get(partitionId);
        if (partitionStat == null) {
            partitionStat = new PartitionStats();
            partitionStats.put(partitionId, partitionStat);
        }
        return partitionStat;
    }

    /**
     * PartitionId is null means...
     *
     * @param tableId table id
     * @param partitionId partition id
     * @return column stats of the partition
     */
    public Map<String, ColumnStats> getColumnStats(long tableId, @Nullable Long partitionId) {
        if (partitionId == null) {
            // TODO(wzt): support for getting column statistics on non-partitioned
            partitionId = tableId;
        }
        PartitionStats partitionStat = getPartitionStats(tableId, partitionId);
        return partitionStat.getNameToColumnStats();
    }

    // TODO: mock statistics need to be removed in the future
    public void mockTableStatsWithRowCount(long tableId, long rowCount) {
        TableStats tableStats = idToTableStats.get(tableId);
        if (tableStats == null) {
            tableStats = new TableStats();
            idToTableStats.put(tableId, tableStats);
        }

        if (tableStats.getRowCount() != rowCount) {
            tableStats.setRowCount(rowCount);
        }
    }
}
