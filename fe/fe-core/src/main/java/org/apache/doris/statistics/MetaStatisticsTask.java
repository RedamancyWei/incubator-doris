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

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
            StatsCategory category = statsDesc.getCategory();
            StatsGranularity granularity = statsDesc.getGranularity();
            List<StatsType> statsTypes = statsDesc.getStatsTypes();

            for (StatsType statsType : statsTypes) {
                switch (statsType) {
                    case MAX_SIZE:
                    case AVG_SIZE:
                        getColSize(category);
                        statsTypeToValue.getOrDefault(statsType, Lists.newArrayList()).add(category);
                        break;
                    case ROW_COUNT:
                        getRowCount(category, granularity);
                        statsTypeToValue.getOrDefault(statsType, Lists.newArrayList()).add(category);
                        break;
                    case DATA_SIZE:
                        getDataSize(category, granularity);
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
                             StatsGranularity statsGranularity) throws DdlException {
        long dbId = category.getDbId();
        long tableId = category.getTableId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(tableId);
        StatsGranularity.Granularity granularity = statsGranularity.getGranularity();

        switch (granularity) {
            case TABLE:
                long tblRowCount = olapTable.getRowCount();
                category.setStatsValue(String.valueOf(tblRowCount));
                break;
            case PARTITION:
                Partition partition1 = olapTable.getPartition(statsGranularity.getPartitionId());
                long ptRowCount = partition1.getBaseIndex().getRowCount();
                category.setStatsValue(String.valueOf(ptRowCount));
                break;
            case TABLET:
                Partition partition2 = olapTable.getPartition(statsGranularity.getPartitionId());
                Tablet tablet = partition2.getBaseIndex().getTablet(statsGranularity.getTabletId());
                boolean singleReplica = tablet.getReplicas().size() == 1;
                long tabletRowCount = tablet.getRowCount(singleReplica);
                category.setStatsValue(String.valueOf(tabletRowCount));
                break;
            default:
                throw new DdlException("Unsupported granularity(" + granularity + ").");
        }
    }

    private void getDataSize(StatsCategory category,
                             StatsGranularity statsGranularity) throws DdlException {

        long dbId = category.getDbId();
        long tableId = category.getTableId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(tableId);
        StatsGranularity.Granularity granularity = statsGranularity.getGranularity();

        switch (granularity) {
            case TABLE:
                long tblDataSize = olapTable.getDataSize();
                category.setStatsValue(String.valueOf(tblDataSize));
                break;
            case PARTITION:
                Partition partition1 = olapTable.getPartition(statsGranularity.getPartitionId());
                long dataSize1 = partition1.getBaseIndex().getDataSize();
                category.setStatsValue(String.valueOf(dataSize1));
                break;
            case TABLET:
                Partition partition2 = olapTable.getPartition(statsGranularity.getPartitionId());
                Tablet tablet = partition2.getBaseIndex().getTablet(statsGranularity.getTabletId());
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
        long tableId = category.getTableId();
        Table table = db.getTableOrDdlException(tableId);
        String columnName = category.getColumnName();
        Column column = table.getColumn(columnName);
        int colSize = column.getDataType().getSlotSize();
        category.setStatsValue(String.valueOf(colSize));
    }
}
