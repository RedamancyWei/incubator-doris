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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;

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

    public AnalyzeStmt(TableName dbTableName, List<String> columns, Map<String, String> properties) {
        this.dbTableName = dbTableName;
        this.columnNames = columns;
        this.properties = properties;
    }

    public List<String> getColumnNames() {
        return this.columnNames;
    }

    public TableName getTableName() {
        return this.dbTableName;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        PaloAuth auth = analyzer.getCatalog().getAuth();
        UserIdentity userInfo = this.getUserInfo();

        // step1: analyze database and table
        if (dbTableName != null) {
            String dbName = dbTableName.getDb();
            if (StringUtils.isNotBlank(dbName)) {
                dbName = analyzer.getClusterName() + ":" + dbName;
            } else {
                dbName = analyzer.getDefaultDb();
            }

            // check database
            Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(dbName);
            if (db == null) {
                throw new AnalysisException("The database(" + dbName + ") does not exist.");
            }

            // check table
            String tblName = dbTableName.getTbl();
            if (StringUtils.isNotBlank(tblName)) {
                Table table = db.getOlapTableOrAnalysisException(tblName);
                if (table == null) {
                    throw new AnalysisException("The table(" + dbTableName.getTbl() + ") does not exist.");
                }
                // check the table permission
                boolean isPermission = auth.checkTblPriv(userInfo, dbName, table.getName(), PrivPredicate.SELECT);
                if (!isPermission) {
                    throw new AnalysisException("You do not have permissions to analyze the table(" + table.getName() + ").");
                }
            } else {
                // check the db permission
                String dbFullName = db.getFullName();
                boolean isPermission = auth.checkDbPriv(userInfo, dbFullName, PrivPredicate.SELECT);
                if (!isPermission) {
                    throw new AnalysisException("You do not have permissions to analyze the database(" + dbFullName + ").");
                }
            }

            // check column
            if (columnNames != null) {
                Table table = db.getOlapTableOrAnalysisException(tblName);
                for (String columnName : columnNames) {
                    Column column = table.getColumn(columnName);
                    if (column == null) {
                        throw new AnalysisException("The column(" + columnName + ") does not exist.");
                    }
                }
            } else {
                columnNames = Lists.newArrayList();
                if (StringUtils.isNotBlank(tblName)) {
                    Table table = db.getOlapTableOrAnalysisException(tblName);
                    List<Column> baseSchema = table.getBaseSchema();
                    baseSchema.stream().map(Column::getName).forEach(name -> columnNames.add(name));
                }
            }
        } else {
            // analyze the default db
            String dbName = analyzer.getDefaultDb();
            Database db = analyzer.getCatalog().getDbOrAnalysisException(dbName);
            String dbFullName = db.getFullName();
            boolean isPermission = auth.checkDbPriv(userInfo, dbFullName, PrivPredicate.SELECT);
            if (!isPermission) {
                throw new AnalysisException("You do not have permissions to analyze the database(" + dbFullName + ").");
            }
            dbTableName = new TableName(dbName, "");
        }

        // step2: analyze properties
        if (properties == null){
            // TODO set default properties
            properties = Maps.newHashMap();
        } else {
            for (Map.Entry<String, String> pros : properties.entrySet()) {
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
}

