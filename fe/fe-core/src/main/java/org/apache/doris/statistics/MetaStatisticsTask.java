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
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.DdlException;
import org.apache.doris.statistics.StatisticsTaskResult.TaskResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;

/**
 * A statistics task that directly collects statistics by reading FE meta.
 * e.g. for fixed-length types such as Int type and Long type we get their size from metadata.
 * 1.The granularity of row count can be table or partition, and the type should be table or partition
 * 2.The granularity of data size can be table or partition, and the type should be table or partition
 * 3.The granularity of max and min size can be table or partition, and the type should be column
 */
public class MetaStatisticsTask extends StatisticsTask {
    public MetaStatisticsTask(long jobId, List<StatisticsDesc> statsDescs) {
        super(jobId, statsDescs);
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        List<TaskResult> taskResults = Lists.newArrayList();

        for (StatisticsDesc statsDesc : statsDescs) {
            checkStatisticsDesc(statsDesc);
            StatsCategory category = statsDesc.getCategory();
            StatsGranularity granularity = statsDesc.getGranularity();
            TaskResult result = createNewTaskResult(category, granularity);
            List<StatsType> statsTypes = statsDesc.getStatsTypes();

            for (StatsType statsType : statsTypes) {
                switch (statsType) {
                    case MAX_SIZE:
                    case AVG_SIZE:
                        getColSize(category, statsType, granularity, result);
                        break;
                    case ROW_COUNT:
                        getRowCount(category.getDbId(), category.getTableId(), granularity, result);
                        break;
                    case DATA_SIZE:
                        getDataSize(category.getDbId(), category.getTableId(), granularity, result);
                        break;
                    default:
                        throw new DdlException("Unsupported statistics type(" + statsType + ").");
                }
            }

            taskResults.add(result);
        }

        return new StatisticsTaskResult(taskResults);
    }

    private void getColSize(StatsCategory category, StatsType statsType, StatsGranularity granularity,
                            TaskResult result) throws DdlException {
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(category.getDbId());
        OlapTable table = (OlapTable) db.getTableOrDdlException(category.getTableId());
        Column column = getNotNullColumn(table, category.getColumnName());
        int colSize = column.getDataType().getSlotSize();

        result.setTableId(granularity.getTableId());
        result.setColumnName(category.getColumnName());

        switch (granularity.getGranularity()) {
            case TABLE:
                result.getStatsTypeToValue().put(statsType, String.valueOf(colSize));
                break;
            case PARTITION:
                Partition partition = getNotNullPartition(granularity, table);
                result.setPartitionName(partition.getName());
                result.getStatsTypeToValue().put(statsType, String.valueOf(colSize));
                break;
            default:
                // the granularity of max and min size only can be table or partition
                throw new DdlException("Unsupported granularity(" + granularity + ").");
        }
    }

    private void getRowCount(long dbId, long tableId, StatsGranularity granularity,
                             TaskResult result) throws DdlException {
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        OlapTable table = (OlapTable) db.getTableOrDdlException(tableId);

        result.setTableId(granularity.getTableId());

        switch (granularity.getGranularity()) {
            case TABLE:
                long tblRowCount = table.getRowCount();
                result.getStatsTypeToValue().put(StatsType.ROW_COUNT, String.valueOf(tblRowCount));
                break;
            case PARTITION:
                Partition partition = getNotNullPartition(granularity, table);
                result.setPartitionName(partition.getName());
                long ptRowCount = partition.getBaseIndex().getRowCount();
                result.getStatsTypeToValue().put(StatsType.ROW_COUNT, String.valueOf(ptRowCount));
                break;
            case TABLET:
                Partition tabletPartition = getNotNullPartition(granularity, table);
                result.setPartitionName(tabletPartition.getName());
                Tablet tablet = getNotNullTablet(granularity, tabletPartition);
                long tabletRowCount = tablet.getRowCount(true);
                result.getStatsTypeToValue().put(StatsType.ROW_COUNT, String.valueOf(tabletRowCount));
                break;
            default:
                throw new DdlException("Unsupported granularity(" + granularity + ").");
        }
    }

    private void getDataSize(long dbId, long tableId, StatsGranularity granularity,
                             TaskResult result) throws DdlException {
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        OlapTable table = (OlapTable) db.getTableOrDdlException(tableId);

        result.setTableId(granularity.getTableId());

        switch (granularity.getGranularity()) {
            case TABLE:
                long tblDataSize = table.getDataSize();
                result.getStatsTypeToValue().put(StatsType.DATA_SIZE, String.valueOf(tblDataSize));
                break;
            case PARTITION:
                Partition partition = getNotNullPartition(granularity, table);
                result.setPartitionName(partition.getName());
                long partitionSize = partition.getBaseIndex().getDataSize();
                result.getStatsTypeToValue().put(StatsType.DATA_SIZE, String.valueOf(partitionSize));
                break;
            case TABLET:
                Partition tabletPartition = getNotNullPartition(granularity, table);
                result.setPartitionName(tabletPartition.getName());
                Tablet tablet = getNotNullTablet(granularity, tabletPartition);
                long tabletSize = tablet.getDataSize(false);
                result.getStatsTypeToValue().put(StatsType.DATA_SIZE, String.valueOf(tabletSize));
                break;
            default:
                throw new DdlException("Unsupported granularity(" + granularity + ").");
        }
    }

    private Partition getNotNullPartition(StatsGranularity granularity, OlapTable olapTable) throws DdlException {
        Partition partition = olapTable.getPartition(granularity.getPartitionId());
        if (partition == null) {
            throw new DdlException("Partition(" + granularity.getPartitionId() + ") not found.");
        }
        return partition;
    }

    private Tablet getNotNullTablet(StatsGranularity granularity, Partition partition) throws DdlException {
        Tablet tablet = partition.getBaseIndex().getTablet(granularity.getTabletId());
        if (tablet == null) {
            throw new DdlException("Tablet(" + granularity.getTabletId() + ") not found.");
        }
        return tablet;
    }

    private Column getNotNullColumn(Table table, String colName) throws DdlException {
        Column column = table.getColumn(colName);
        if (column == null) {
            throw new DdlException("Column(" + colName + ") not found.");
        }
        return column;
    }

    private void checkStatisticsDesc(StatisticsDesc statisticsDesc) throws DdlException {
        if (statisticsDesc == null) {
            throw new DdlException("StatisticsDesc is null.");
        }

        if (statisticsDesc.getCategory() == null) {
            throw new DdlException("Category is null.");
        }

        if (statisticsDesc.getGranularity() == null) {
            throw new DdlException("Granularity is null.");
        }

        Preconditions.checkState(statisticsDesc.getCategory().getDbId() > 0L);
        Preconditions.checkState(statisticsDesc.getCategory().getTableId() > 0L);
    }

    private TaskResult createNewTaskResult(StatsCategory category, StatsGranularity granularity) {
        TaskResult result = new TaskResult();
        result.setDbId(category.getDbId());
        result.setCategory(category.getCategory());
        result.setGranularity(granularity.getGranularity());
        result.setStatsTypeToValue(Maps.newHashMap());
        return result;
    }
}
