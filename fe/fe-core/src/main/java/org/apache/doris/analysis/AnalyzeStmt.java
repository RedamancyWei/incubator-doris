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
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import org.apache.parquet.Preconditions;
import org.apache.parquet.Strings;

import java.util.List;
import java.util.Map;

/**
 * Collect statistics about a database
 * <p>
 * syntax:
 * ANALYZE [[ db_name.tb_name ] [( column_name [, ...] )], ...] [ PROPERTIES(...) ]
 * <p>
 * db_name.tb_name: collect table and column statistics from tb_name
 * <p>
 * column_name: collect column statistics from column_name
 * <p>
 * properties: properties of statistics jobs
 */
public class AnalyzeStmt extends DdlStmt {
    private TableName dbTableName;
    private List<String> columnNames;
    private Map<String, String> properties;

    // after analyzed
    private String dbName;
    private String tblName;

    public AnalyzeStmt(TableName dbTableName, List<String> columns, Map<String, String> properties) {
        this.dbTableName = dbTableName;
        this.columnNames = columns;
        this.properties = properties;
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

    public List<String> getColumnNames() {
        return this.columnNames;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // step1: analyze database and table
        if (this.dbTableName != null) {
            this.dbName = this.dbTableName.getDb();
            if (Strings.isNullOrEmpty(this.dbName)) {
                this.dbName = analyzer.getDefaultDb();
            } else {
                this.dbName = ClusterNamespace.getFullName(analyzer.getClusterName(), this.dbName);
            }

            // check database
            Database db = analyzer.getCatalog().getDbOrAnalysisException(this.dbName);
            if (db == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }

            // check table
            this.tblName = this.dbTableName.getTbl();
            if (Strings.isNullOrEmpty(this.tblName)) {
                List<Table> tables = db.getTables();
                for (Table table : tables) {
                    checkAnalyzePriv(this.dbName, table.getName());
                }
            } else {
                Table table = db.getTableOrAnalysisException(this.tblName);
                if (table == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_SUCH_TABLE);
                }
                checkAnalyzePriv(this.dbName, this.tblName);
            }

            // check column
            if (this.columnNames != null) {
                Table table = db.getOlapTableOrAnalysisException(this.tblName);
                for (String columnName : this.columnNames) {
                    Column column = table.getColumn(columnName);
                    if (column == null) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME, columnName);
                    }
                }
            } else {
                this.columnNames = Lists.newArrayList();
                if (Strings.isNullOrEmpty(this.tblName)) {
                    Table table = db.getOlapTableOrAnalysisException(this.tblName);
                    List<Column> baseSchema = table.getBaseSchema();
                    baseSchema.stream().map(Column::getName).forEach(name -> this.columnNames.add(name));
                }
            }
        } else {
            // analyze the default db
            this.dbName = analyzer.getDefaultDb();
            Database db = analyzer.getCatalog().getDbOrAnalysisException(this.dbName);
            List<Table> tables = db.getTables();
            for (Table table : tables) {
                checkAnalyzePriv(this.dbName, table.getName());
            }
        }

        // step2: analyze properties
        if (this.properties == null) {
            // TODO set default properties
            this.properties = Maps.newHashMap();
        } else {
            for (Map.Entry<String, String> pros : this.properties.entrySet()) {
                // TODO check key & value
                String key = pros.getKey();
                String value = pros.getValue();
            }
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    public void checkAnalyzePriv(String dbName, String tblName) throws AnalysisException {
        PaloAuth auth = Catalog.getCurrentCatalog().getAuth();
        if (!auth.checkTblPriv(ConnectContext.get(), dbName, tblName, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    "ANALYZE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + ": " + tblName);
        }
    }
}

