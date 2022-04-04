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
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import org.apache.parquet.Preconditions;

public class ShowAnalyzeStmt extends ShowStmt {
    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
                    .add("id")
                    .add("scope")
                    .add("create_time")
                    .add("running_time")
                    .add("finish_time")
                    .add("state")
                    .add("progress")
                    .build();

    private long jobId = -1L;
    private TableName dbTableName;

    // after analyzed
    private String dbName;
    private String tblName;

    public ShowAnalyzeStmt() {
    }

    public ShowAnalyzeStmt(long jobId) {
        this.jobId = jobId;
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

    public long getJobId() {
        return this.jobId;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        if (Strings.isNullOrEmpty(this.dbTableName.getDb())) {
            this.dbName = analyzer.getDefaultDb();
        } else {
            this.dbName = ClusterNamespace.getFullName(analyzer.getClusterName(), this.dbName);
        }

        PaloAuth auth = analyzer.getCatalog().getAuth();
        UserIdentity userInfo = this.getUserInfo();
        Database database = analyzer.getCatalog().getDbOrAnalysisException(this.dbName);

        if (Strings.isNullOrEmpty(this.dbTableName.getTbl())) {
            if (database == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            boolean dbPermission = auth.checkDbPriv(userInfo, this.dbName, PrivPredicate.SHOW);
            if (!dbPermission) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR);
            }
        } else {
            Table table = database.getTableOrAnalysisException(this.tblName);
            if (table == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_SUCH_TABLE);
            }
            boolean tblPermission = auth.checkTblPriv(userInfo, this.dbName, table.getName(), PrivPredicate.SHOW);
            if (!tblPermission) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR);
            }
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
