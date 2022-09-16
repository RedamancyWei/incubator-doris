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

package org.apache.doris.statistics.util;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.planner.OriginalPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Mainly used for statistics module. FE obtains statistics by SQL query BE,
 * such as column maximum value, minimum value, etc.
 **/
public class InternalQuery {
    private static final Logger LOG = LogManager.getLogger(InternalQuery.class);

    private final String database;
    private final String sql;
    private ConnectContext context;
    private Coordinator coord;

    private StatementBase stmt;
    private final List<TResultBatch> resultBatches = Lists.newArrayList();

    public InternalQuery(String database, String sql) {
        this.database = database;
        this.sql = sql;
    }

    /**
     * 123
     *
     * @return 123
     * @throws Exception 123
     */
    public InternalQueryResult query() throws Exception {
        // step1:
        buildContext();

        // step2: parse sql
        parseSql();

        // step3: generate plan
        prepare();

        // step4: execute and get result
        execute();

        // step5: parse result data adn return
        return fetchResult();
    }

    private void buildContext() {
        context = new ConnectContext();
        context.setEnv(Env.getCurrentEnv());
        context.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());

        String fullDbName = ClusterNamespace
                .getFullName(SystemInfoService.DEFAULT_CLUSTER, database);
        context.setDatabase(fullDbName);

        UUID uuid = UUID.randomUUID();
        TUniqueId newQueryId = new TUniqueId(uuid.getMostSignificantBits(),
                uuid.getLeastSignificantBits());
        context.setQueryId(newQueryId);

        context.setThreadLocalInfo();
        context.setStartTime();
    }

    private void parseSql() throws DdlException {
        SqlScanner input = new SqlScanner(new StringReader(sql),
                context.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        try {
            stmt = SqlParserUtils.getFirstStmt(parser);
            stmt.setOrigStmt(new OriginStatement(sql, 0));
        } catch (Exception e) {
            LOG.warn("Failed to parse the internal statement: {}. {}", sql, e);
            throw new DdlException("Failed to parse the internal statement:" + sql);
        }
    }

    private void prepare() throws UserException {
        Analyzer analyzer = new Analyzer(context.getEnv(), context);
        stmt.analyze(analyzer);

        OriginalPlanner originalPlanner = new OriginalPlanner(stmt.getAnalyzer());
        TQueryOptions queryOptions = new TQueryOptions();
        originalPlanner.plan(stmt, queryOptions);

        coord = new Coordinator(context, analyzer, originalPlanner);
    }

    private void execute() throws Exception {
        TUniqueId tUniqueId = context.queryId();
        try {
            QeProcessorImpl.INSTANCE.registerQuery(tUniqueId, coord);
            coord.exec();
            if (coord.getExecStatus().ok()) {
                RowBatch batch;
                do {
                    batch = coord.getNext();
                    if (batch.getBatch() != null) {
                        resultBatches.add(batch.getBatch());
                    }
                } while (!batch.isEos());
            } else {
                coord.cancel();
                String errMsg = coord.getExecStatus().getErrorMsg();
                ErrorReport.reportDdlException(errMsg, ErrorCode.ERR_QUERY_INTERRUPTED);
            }
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(tUniqueId);
        }
    }

    private InternalQueryResult fetchResult() throws DdlException {
        List<String> columns = stmt.getColLabels();
        List<PrimitiveType> types = stmt.getResultExprs().stream()
                .map(e -> e.getType().getPrimitiveType())
                .collect(Collectors.toList());

        List<ResultRow> resultRows = Lists.newArrayList();
        InternalQueryResult result = new InternalQueryResult(columns, types);

        for (TResultBatch batch : resultBatches) {
            List<ByteBuffer> rows = batch.getRows();
            for (ByteBuffer buffer : rows) {
                List<String> values = Lists.newArrayList();
                InternalQueryBuffer queryBuffer = new InternalQueryBuffer(buffer.slice());

                for (int i = 0; i < columns.size(); i++) {
                    String value = queryBuffer.readStringWithLength();
                    values.add(value);
                }

                ResultRow resultRow = new ResultRow(values);
                resultRows.add(resultRow);
            }

            result.setResultRows(resultRows);
        }

        return result;
    }

    private void cancel() {
        if (!coord.isDone()) {
            coord.cancel();
            LOG.info("Internal query has been cancelled: {}", sql);
        }
    }
}
