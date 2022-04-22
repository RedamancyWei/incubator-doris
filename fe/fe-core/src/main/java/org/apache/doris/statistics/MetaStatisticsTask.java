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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/*
A statistics task that directly collects statistics by reading FE meta.
 */
public class MetaStatisticsTask extends StatisticsTask {

    public MetaStatisticsTask(long jobId,
                              StatsGranularityDesc granularityDesc,
                              StatsCategoryDesc categoryDesc,
                              List<StatsType> statsTypeList) {
        super(jobId, granularityDesc, categoryDesc, statsTypeList);
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        Map<StatsType, String> statsTypeToValue = Maps.newHashMap();
        List<StatsType> statsTypeList = this.getStatsTypeList();

        for (StatsType statsType : statsTypeList) {
            switch (statsType) {
                case ROW_COUNT:
                    getRowCount(statsType, statsTypeToValue);
                    break;
                case DATA_SIZE:
                    getDataSize(statsType, statsTypeToValue);
                    break;
                case MAX_SIZE:
                case AVG_SIZE:
                    getColSize(statsType, statsTypeToValue);
                    break;
                default:
                    throw new DdlException("unsupported type(" + statsType + ").");
            }
        }

        return new StatisticsTaskResult(this.granularityDesc, this.categoryDesc, statsTypeToValue);
    }

    private void getRowCount(StatsType statsType, Map<StatsType, String> statsTypeToValue) throws DdlException {
        StatsCategoryDesc categoryDesc = this.getCategoryDesc();
        long dbId = categoryDesc.getDbId();
        long tableId = categoryDesc.getTableId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(tableId);
        long rowCount = olapTable.getRowCount();
        statsTypeToValue.put(statsType, String.valueOf(rowCount));
    }

    private void getDataSize(StatsType statsType, Map<StatsType, String> statsTypeToValue) throws DdlException {
        StatsCategoryDesc categoryDesc = this.getCategoryDesc();
        long dbId = categoryDesc.getDbId();
        long tableId = categoryDesc.getTableId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(tableId);
        long dataSize = olapTable.getDataSize();
        statsTypeToValue.put(statsType, String.valueOf(dataSize));
    }

    private void getColSize(StatsType statsType, Map<StatsType, String> statsTypeToValue) throws DdlException {
        StatsCategoryDesc categoryDesc = this.getCategoryDesc();
        long dbId = categoryDesc.getDbId();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        long tableId = categoryDesc.getTableId();
        Table table = db.getTableOrDdlException(tableId);
        String columnName = categoryDesc.getColumnName();
        Column column = table.getColumn(columnName);
        int typeSize = column.getDataType().getSlotSize();
        statsTypeToValue.put(statsType, String.valueOf(typeSize));
    }
}
