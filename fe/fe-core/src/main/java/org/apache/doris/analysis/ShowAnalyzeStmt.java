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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class ShowAnalyzeStmt extends ShowStmt {
    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("id")
            .add("create_time")
            .add("running_time")
            .add("finish_time")
            .add("scope")
            .add("progress")
            .add("state")
            .build();

    private List<Long> jobIds;
    private TableName dbTableName;

    // after analyzed
    private String dbName;
    private String tblName;

    public ShowAnalyzeStmt() {
    }

    public ShowAnalyzeStmt(List<Long> jobIds) {
        this.jobIds = jobIds;
    }

    public ShowAnalyzeStmt(TableName dbTableName) {
        this.dbTableName = dbTableName;
    }

    public String getDbName() {
        Preconditions.checkArgument(isAnalyzed(),
                "The db name must be obtained after the parsing is complete");
        return this.dbName;
    }

    public String getTblName() {
        Preconditions.checkArgument(isAnalyzed(),
                "The tbl name must be obtained after the parsing is complete");
        return this.tblName;
    }

    public List<Long> getJobIds() {
        return this.jobIds;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        if (this.dbTableName != null) {
            this.dbName = this.dbTableName.getDb();
            this.tblName = this.dbTableName.getTbl();
        }

        if (Strings.isNullOrEmpty(this.dbName)) {
            this.dbName = analyzer.getDefaultDb();
        } else {
            this.dbName = ClusterNamespace.getFullName(analyzer.getClusterName(), this.dbName);
        }

        if (Strings.isNullOrEmpty(this.dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }
        Database db = analyzer.getCatalog().getDbOrAnalysisException(this.dbName);

        if (Strings.isNullOrEmpty(this.tblName)) {
            List<Table> tables = db.getTables();
            for (Table tbl : tables) {
                checkShowAnalyzePriv(this.dbName, tbl.getName());
            }
        } else {
            Table tbl = db.getTableOrAnalysisException(this.tblName);
            if (tbl == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_SUCH_TABLE);
            }
            checkShowAnalyzePriv(this.dbName, this.tblName);
        }

    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(128)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    private void checkShowAnalyzePriv(String dbName, String tblName) throws AnalysisException {
        PaloAuth auth = Catalog.getCurrentCatalog().getAuth();
        if (!auth.checkTblPriv(ConnectContext.get(), dbName, tblName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    "SHOW ANALYZE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + ": " + tblName);
        }
    }
}
