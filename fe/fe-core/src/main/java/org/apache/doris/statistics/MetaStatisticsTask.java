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

import com.google.common.collect.Maps;
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
        Map<StatsType, String> statsTypeToValue = Maps.newHashMap();

        for (StatisticsDesc statsDesc : statsDescs) {
            StatsCategory category = statsDesc.getCategory();
            StatsGranularity granularity = statsDesc.getGranularity();
            List<StatsType> statsTypes = statsDesc.getStatsTypes();

            for (StatsType statsType : statsTypes) {
                switch (statsType) {
                    case MAX_SIZE:
                    case AVG_SIZE:
                        getColSize(category, statsType, statsTypeToValue);
                        break;
                    case ROW_COUNT:
                        getRowCount(category, granularity, statsType, statsTypeToValue);
                        break;
                    case DATA_SIZE:
                        getDataSize(category, granularity, statsType, statsTypeToValue);
                        break;
                    default:
                        throw new DdlException("Unsupported statistics type(" + statsType + ").");
                }
            }
        }

        return new StatisticsTaskResult(statsDescs, statsTypeToValue);
    }

    private void getRowCount(StatsCategory category,
                             StatsGranularity statsGranularity,
                             StatsType statsType,
                             Map<StatsType, String> statsTypeToValue) throws DdlException {
        long dbId = category.getDbId();
        long tableId = category.getTableId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(tableId);
        StatsGranularity.Granularity granularity = statsGranularity.getGranularity();

        switch (granularity) {
            case TABLE:
                long tblRowCount = olapTable.getRowCount();
                statsTypeToValue.put(statsType, String.valueOf(tblRowCount));
                break;
            case PARTITION:
                Partition pPartition = olapTable.getPartition(statsGranularity.getPartitionId());
                long ptRowCount = pPartition.getBaseIndex().getRowCount();
                // if the same stats type is used in multiple partitions, merge the row count value.
                String ptValue = statsTypeToValue.getOrDefault(statsType, "0");
                ptRowCount = Long.parseLong(ptValue) + ptRowCount;
                statsTypeToValue.put(statsType, String.valueOf(ptRowCount));
                break;
            case TABLET:
                Partition tPartition = olapTable.getPartition(statsGranularity.getPartitionId());
                Tablet tablet = tPartition.getBaseIndex().getTablet(statsGranularity.getTabletId());
                boolean singleReplica = tablet.getReplicas().size() == 1;
                long tabletRowCount = tablet.getRowCount(singleReplica);
                statsTypeToValue.put(statsType, String.valueOf(tabletRowCount));
                // if the same stats type is used in multiple tablet, merge the row count value.
                String defaultValue1 = statsTypeToValue.getOrDefault(statsType, "0");
                tabletRowCount = Long.parseLong(defaultValue1) + tabletRowCount;
                statsTypeToValue.put(statsType, String.valueOf(tabletRowCount));
                break;
            default:
                throw new DdlException("Unsupported granularity(" + granularity + ").");
        }
    }

    private void getDataSize(StatsCategory category,
                             StatsGranularity statsGranularity,
                             StatsType statsType,
                             Map<StatsType, String> statsTypeToValue) throws DdlException {

        long dbId = category.getDbId();
        long tableId = category.getTableId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(tableId);
        StatsGranularity.Granularity granularity = statsGranularity.getGranularity();

        switch (granularity) {
            case TABLE:
                long tblDataSize = olapTable.getDataSize();
                statsTypeToValue.put(statsType, String.valueOf(tblDataSize));
                break;
            case PARTITION:
                Partition pPartition = olapTable.getPartition(statsGranularity.getPartitionId());
                long pDataSize = pPartition.getBaseIndex().getDataSize();
                // if the same stats type is used in multiple partitions, merge the row count value.
                String pValue = statsTypeToValue.getOrDefault(statsType, "0");
                pDataSize = Long.parseLong(pValue) + pDataSize;
                statsTypeToValue.put(statsType, String.valueOf(pDataSize));
                break;
            case TABLET:
                Partition tPartition = olapTable.getPartition(statsGranularity.getPartitionId());
                Tablet tablet = tPartition.getBaseIndex().getTablet(statsGranularity.getTabletId());
                boolean singleReplica = tablet.getReplicas().size() == 1;
                long tDataSize = tablet.getDataSize(singleReplica);
                statsTypeToValue.put(statsType, String.valueOf(tDataSize));
                // if the same stats type is used in multiple tablet, merge the row count value.
                String tValue = statsTypeToValue.getOrDefault(statsType, "0");
                tDataSize = Long.parseLong(tValue) + tDataSize;
                statsTypeToValue.put(statsType, String.valueOf(tDataSize));
                break;
            default:
                throw new DdlException("Unsupported granularity(" + granularity + ").");
        }
    }

    private void getColSize(StatsCategory category,
                            StatsType statsType,
                            Map<StatsType, String> statsTypeToValue) throws DdlException {
        long dbId = category.getDbId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        long tableId = category.getTableId();
        Table table = db.getTableOrDdlException(tableId);
        String columnName = category.getColumnName();
        Column column = table.getColumn(columnName);
        int typeSize = column.getDataType().getSlotSize();
        statsTypeToValue.put(statsType, String.valueOf(typeSize));
    }
}
