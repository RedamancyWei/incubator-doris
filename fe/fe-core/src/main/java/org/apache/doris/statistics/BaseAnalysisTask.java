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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisType;
import org.apache.doris.system.SystemInfoService;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseAnalysisTask {

    public static final Logger LOG = LogManager.getLogger(BaseAnalysisTask.class);

    protected AnalysisTaskScheduler analysisTaskScheduler;

    protected AnalysisTaskInfo info;

    protected CatalogIf catalog;

    protected DatabaseIf db;

    protected TableIf tbl;

    protected Column col;

    protected StmtExecutor stmtExecutor;

    protected AnalysisState analysisState;

    @VisibleForTesting
    public BaseAnalysisTask() {

    }

    public BaseAnalysisTask(AnalysisTaskScheduler analysisTaskScheduler, AnalysisTaskInfo info) {
        this.analysisTaskScheduler = analysisTaskScheduler;
        this.info = info;
        init(info);
    }

    private void init(AnalysisTaskInfo info) {
        catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(info.catalogName);
        if (catalog == null) {
            Env.getCurrentEnv().getAnalysisManager().updateTaskStatus(info, AnalysisState.FAILED,
                    String.format("Catalog with name: %s not exists", info.dbName), System.currentTimeMillis());
            return;
        }
        db = (DatabaseIf) catalog.getDb(info.dbName).orElse(null);
        if (db == null) {
            Env.getCurrentEnv().getAnalysisManager().updateTaskStatus(info, AnalysisState.FAILED,
                    String.format("DB with name %s not exists", info.dbName), System.currentTimeMillis());
            return;
        }
        tbl = (TableIf) db.getTable(info.tblName).orElse(null);
        if (tbl == null) {
            Env.getCurrentEnv().getAnalysisManager().updateTaskStatus(
                    info, AnalysisState.FAILED,
                    String.format("Table with name %s not exists", info.tblName), System.currentTimeMillis());
        }
        if (info.analysisType != null && (info.analysisType.equals(AnalysisType.COLUMN)
                || info.analysisType.equals(AnalysisType.HISTOGRAM))) {
            col = tbl.getColumn(info.colName);
            if (col == null) {
                Env.getCurrentEnv().getAnalysisManager().updateTaskStatus(
                        info, AnalysisState.FAILED, String.format("Column with name %s not exists", info.tblName),
                        System.currentTimeMillis());
            }
        }
    }

    public abstract void execute() throws Exception;

    public void cancel() {
        if (stmtExecutor != null) {
            stmtExecutor.cancel();
        }
        Env.getCurrentEnv().getAnalysisManager()
                .updateTaskStatus(info, AnalysisState.FAILED,
                        String.format("Job has been cancelled: %s", info.toString()), -1);
    }

    public int getLastExecTime() {
        return info.lastExecTimeInMs;
    }

    public long getJobId() {
        return info.jobId;
    }

    public Map<String, String> getBaseParams() {
        Map<String, String> params = new HashMap<>();
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", info.indexId == null ?
                "-1" : String.valueOf(info.indexId));
        params.put("colId", String.valueOf(info.colName));
        params.put("dbName", info.dbName);
        params.put("tblName", String.valueOf(info.tblName));
        params.put("colName", String.valueOf(info.colName));
        params.put("dataSizeFunction", getDataSizeFunction(col));
        return params;
    }

    public AnalysisState getAnalysisState() {
        return analysisState;
    }

    protected String getDataSizeFunction(Column column) {
        if (column.getType().isStringType()) {
            return "SUM(LENGTH(`${colName}`))";
        }
        return "COUNT(1) * " + column.getType().getSlotSize();
    }

}
