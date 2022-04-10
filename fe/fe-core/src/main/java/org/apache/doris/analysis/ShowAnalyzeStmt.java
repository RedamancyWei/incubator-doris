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
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.StatisticsJob;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * ShowAnalyzeStmt is used to show statistics job info.
 * <p>
 * Grammar:
 * SHOW ANALYZE
 *     [TABLE | ID]
 *     [
 *         WHERE
 *         [STATE = ["PENDING"|"SCHEDULING"|"RUNNING"|"FINISHED"|"FAILED"|"CANCELLED"]]
 *     ]
 *     [ORDER BY ...]
 *     [LIMIT limit][OFFSET offset];
 */
public class ShowAnalyzeStmt extends ShowStmt {
    private static final String STATE_NAME = "state";
    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("id")
            .add("create_time")
            .add("start_time")
            .add("finish_time")
            .add("error_msg")
            .add("scope")
            .add("progress")
            .add("state")
            .build();

    private List<Long> jobIds;
    private TableName dbTableName;
    private Expr whereClause;
    private LimitElement limitElement;
    private List<OrderByElement> orderByElements;

    // after analyzed
    private Database db;
    private Table table;
    private String stateValue;
    private ArrayList<OrderByPair> orderByPairs;

    public ShowAnalyzeStmt() {
    }

    public ShowAnalyzeStmt(List<Long> jobIds) {
        this.jobIds = jobIds;
    }

    public ShowAnalyzeStmt(TableName dbTableName, Expr whereClause, List<OrderByElement> orderByElements, LimitElement limitElement) {
        this.dbTableName = dbTableName;
        this.whereClause = whereClause;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
    }

    public List<Long> getJobIds() {
        return this.jobIds;
    }

    public Database getDb() {
        Preconditions.checkArgument(isAnalyzed(),
                "The db name must be obtained after the parsing is complete");
        return this.db;
    }

    public Table getTable() {
        Preconditions.checkArgument(isAnalyzed(),
                "The tbl name must be obtained after the parsing is complete");
        return this.table;
    }

    public String getStateValue() {
        Preconditions.checkArgument(isAnalyzed(),
                "The tbl name must be obtained after the parsing is complete");
        return this.stateValue;
    }

    public ArrayList<OrderByPair> getOrderByPairs() {
        Preconditions.checkArgument(isAnalyzed(),
                "The tbl name must be obtained after the parsing is complete");
        return this.orderByPairs;
    }

    public long getLimit() {
        if (this.limitElement != null && this.limitElement.hasLimit()) {
            return this.limitElement.getLimit();
        }
        return -1L;
    }

    public long getOffset() {
        if (this.limitElement != null && this.limitElement.hasOffset()) {
            return this.limitElement.getOffset();
        }
        return -1L;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        String dbName = null;
        String tblName = null;
        if (this.dbTableName != null) {
            dbName = this.dbTableName.getDb();
            tblName = this.dbTableName.getTbl();
        }

        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
        } else {
            dbName = ClusterNamespace.getFullName(analyzer.getClusterName(), dbName);
        }

        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }
        this.db = analyzer.getCatalog().getDbOrAnalysisException(dbName);

        if (Strings.isNullOrEmpty(tblName)) {
            this.table = null;
            List<Table> tables = this.db.getTables();
            for (Table table : tables) {
                checkShowAnalyzePriv(dbName, table.getName());
            }
        } else {
            this.table = this.db.getTableOrAnalysisException(tblName);
            checkShowAnalyzePriv(dbName, this.table.getName());
        }

        // analyze where clause if not null
        if (this.whereClause != null) {
            if (this.whereClause instanceof CompoundPredicate) {
                CompoundPredicate cp = (CompoundPredicate) this.whereClause;
                if (cp.getOp() != CompoundPredicate.Operator.AND) {
                    throw new AnalysisException("Only allow compound predicate with operator AND");
                }
                // check whether left.columnName equals to right.columnName
                checkPredicateName(cp.getChild(0), cp.getChild(1));
                analyzeSubPredicate(cp.getChild(0));
                analyzeSubPredicate(cp.getChild(1));
            } else {
                analyzeSubPredicate(this.whereClause);
            }
        }

        // analyze order by
        if (this.orderByElements != null && !this.orderByElements.isEmpty()) {
            this.orderByPairs = new ArrayList<>();
            for (OrderByElement orderByElement : this.orderByElements) {
                if (!(orderByElement.getExpr() instanceof SlotRef)) {
                    throw new AnalysisException("Should order by column");
                }
                SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                int index = analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                this.orderByPairs.add(orderByPair);
            }
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

    private void checkPredicateName(Expr leftChild, Expr rightChild) throws AnalysisException {
        String leftChildColumnName = ((SlotRef) leftChild.getChild(0)).getColumnName();
        String rightChildColumnName = ((SlotRef) rightChild.getChild(0)).getColumnName();
        if (leftChildColumnName.equals(rightChildColumnName)) {
            throw new AnalysisException("column names on both sides of operator AND should be different");
        }
    }

    private void analyzeSubPredicate(Expr subExpr) throws AnalysisException {
        if (subExpr == null) {
            return;
        }

        boolean valid = true;

        CHECK:
        {
            if (subExpr instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
                if (binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                    valid = false;
                    break CHECK;
                }
            } else {
                valid = false;
                break CHECK;
            }

            // left child
            if (!(subExpr.getChild(0) instanceof SlotRef)) {
                valid = false;
                break CHECK;
            }
            String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName();
            if (!STATE_NAME.equalsIgnoreCase(leftKey)) {
                valid = false;
                break CHECK;
            }

            // right child
            if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                valid = false;
                break CHECK;
            }

            String value = subExpr.getChild(1).getStringValue();
            if (Strings.isNullOrEmpty(value)) {
                valid = false;
                break CHECK;
            }

            this.stateValue = value.toUpperCase();
            try {
                StatisticsJob.JobState.valueOf(this.stateValue);
            } catch (Exception e) {
                valid = false;
            }
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: " +
                    "STATE = \"PENDING|SCHEDULING|RUNNING|FINISHED|FAILED|CANCELLED\", " +
                    "or compound predicate with operator AND");
        }
    }

    private int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return TITLE_NAMES.indexOf(title);
            }
        }
        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ANALYZE ");
        if (this.table != null) {
            sb.append("`").append(this.table.getName()).append("`");
        }

        if (this.whereClause != null) {
            sb.append(" WHERE ").append(this.whereClause.toSql());
        }

        // Order By clause
        if (this.orderByElements != null) {
            sb.append(" ORDER BY ");
            for (int i = 0; i < this.orderByElements.size(); ++i) {
                sb.append(this.orderByElements.get(i).getExpr().toSql());
                sb.append((this.orderByElements.get(i).getIsAsc()) ? " ASC" : " DESC");
                sb.append((i + 1 != this.orderByElements.size()) ? ", " : "");
            }
        }

        if (getLimit() != -1L) {
            sb.append(" LIMIT ").append(getLimit());
        }

        if (getOffset() != -1L) {
            sb.append(" OFFSET ").append(getOffset());
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
