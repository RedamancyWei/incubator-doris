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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

/**
 * A statistics task that directly collects statistics by reading FE meta.
 * e.g. for fixed-length types such as Int type and Long type we get their size from metadata.
 */
public class MetaStatisticsTask extends StatisticsTask {
    public MetaStatisticsTask(long jobId, List<StatisticsDesc> statsDescs) {
        super(jobId, statsDescs);
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        Map<StatsType, List<StatsCategory>> statsTypeToValue = Maps.newHashMap();

        for (StatisticsDesc statsDesc : statsDescs) {
            List<StatsType> statsTypes = statsDesc.getStatsTypes();
            StatsCategory category = statsDesc.getCategory();

            for (StatsType statsType : statsTypes) {
                switch (statsType) {
                    case ROW_COUNT:
                        getRowCount(category, statsDesc.getGranularity());
                        statsTypeToValue.getOrDefault(statsType, Lists.newArrayList()).add(category);
                        break;
                    case DATA_SIZE:
                        getDataSize(category, statsDesc.getGranularity());
                        statsTypeToValue.getOrDefault(statsType, Lists.newArrayList()).add(category);
                        break;
                    case MAX_SIZE:
                    case AVG_SIZE:
                        getColSize(category);
                        statsTypeToValue.getOrDefault(statsType, Lists.newArrayList()).add(category);
                        break;
                    default:
                        throw new DdlException("Unsupported statistics type(" + statsType + ").");
                }
            }
        }

        return new StatisticsTaskResult(statsTypeToValue);
    }

    private void getRowCount(StatsCategory category,
                             StatsGranularity granularity) throws DdlException {
        long dbId = category.getDbId();
        long tableId = category.getTableId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(tableId);

        switch (granularity.getGranularity()) {
            case TABLE:
                long tblRowCount = olapTable.getRowCount();
                category.setStatsValue(String.valueOf(tblRowCount));
                break;
            case PARTITION:
                Partition partition1 = getNotNullPartition(granularity, olapTable);
                long ptRowCount = partition1.getBaseIndex().getRowCount();
                category.setStatsValue(String.valueOf(ptRowCount));
                break;
            case TABLET:
                Partition partition2 = getNotNullPartition(granularity, olapTable);
                Tablet tablet = getNotNullTablet(granularity, partition2);
                boolean singleReplica = tablet.getReplicas().size() == 1;
                long tabletRowCount = tablet.getRowCount(singleReplica);
                category.setStatsValue(String.valueOf(tabletRowCount));
                break;
            default:
                throw new DdlException("Unsupported granularity(" + granularity + ").");
        }
    }

    private void getDataSize(StatsCategory category,
                             StatsGranularity granularity) throws DdlException {

        long dbId = category.getDbId();
        long tableId = category.getTableId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(tableId);

        switch (granularity.getGranularity()) {
            case TABLE:
                long tblDataSize = olapTable.getDataSize();
                category.setStatsValue(String.valueOf(tblDataSize));
                break;
            case PARTITION:
                Partition partition1 = getNotNullPartition(granularity, olapTable);
                long dataSize1 = partition1.getBaseIndex().getDataSize();
                category.setStatsValue(String.valueOf(dataSize1));
                break;
            case TABLET:
                Partition partition2 = getNotNullPartition(granularity, olapTable);
                Tablet tablet = getNotNullTablet(granularity, partition2);
                boolean singleReplica = tablet.getReplicas().size() == 1;
                long dataSize2 = tablet.getDataSize(singleReplica);
                category.setStatsValue(String.valueOf(dataSize2));
                break;
            default:
                throw new DdlException("Unsupported granularity(" + granularity + ").");
        }
    }

    private void getColSize(StatsCategory category) throws DdlException {
        long dbId = category.getDbId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        long tblId = category.getTableId();
        Table table = db.getTableOrDdlException(tblId);
        String colName = category.getColumnName();
        Column column = getNotNullColumn(table, colName);
        int colSize = column.getDataType().getSlotSize();
        category.setStatsValue(String.valueOf(colSize));
    }

    @NotNull
    private Partition getNotNullPartition(StatsGranularity granularity, OlapTable olapTable) throws DdlException {
        Partition partition = olapTable.getPartition(granularity.getPartitionId());
        if (partition == null) {
            throw new DdlException("Partition(" + granularity.getPartitionId() + ") not found.");
        }
        return partition;
    }

    @NotNull
    private Tablet getNotNullTablet(StatsGranularity granularity, Partition partition2) throws DdlException {
        Tablet tablet = partition2.getBaseIndex().getTablet(granularity.getTabletId());
        if (tablet == null) {
            throw new DdlException("Tablet(" + granularity.getTabletId() + ") not found.");
        }
        return tablet;
    }

    @NotNull
    private Column getNotNullColumn(Table table, String colName) throws DdlException {
        Column column = table.getColumn(colName);
        if (column == null) {
            throw new DdlException("Column(" + colName + ") not found.");
        }
        return column;
    }
}
