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


// import com.alibaba.google.common.collect.Lists;
// import java.nio.ByteBuffer;
// import java.util.List;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
// import org.apache.doris.analysis.Analyzer;
// import org.apache.doris.analysis.StatementBase;
// import org.apache.doris.analysis.UserIdentity;
// import org.apache.doris.catalog.Catalog;
// import org.apache.doris.catalog.Database;
// import org.apache.doris.qe.ConnectContext;
// import org.apache.doris.qe.Coordinator;
// import org.apache.doris.qe.RowBatch;
// import org.apache.doris.thrift.TResultBatch;
// import org.apache.iceberg.util.UUIDUtil;
// import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class StatisticsUtils {
    private static final Pattern PATTERN = Pattern.compile("\\$\\{\\w+\\}");

    public static String processTemplate(String template, Map<String, Object> params) {
        Matcher matcher = PATTERN.matcher(template);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String param = matcher.group();
            Object value = params.get(param.substring(2, param.length() - 1));
            matcher.appendReplacement(sb, value == null ? "" : value.toString());
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    public static ConnectContext buildConnectContext() {
        ConnectContext context = new ConnectContext();
        // Note: statistics query does not register query id to QeProcessorImpl::coordinatorMap,
        // but QeProcessorImpl::reportExecStatus will check query id,
        // So we must disable report query status from BE to FE
        // context.getSessionVariable().setReportSuccess(false);
        // Always use 1 parallel to avoid affect normal query
        // context.getSessionVariable().setParallelExecInstanceNum(1);
        // TODO(kks): remove this if pipeline support STATISTIC result sink type
        // context.getSessionVariable().setEnablePipelineEngine(false);
        context.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        context.setDatabase("default");
        context.setCatalog(Catalog.getCurrentCatalog());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        context.setQueryId(queryId);
        // context.setExecutionId(UUIDUtil.toTUniqueId(context.getQueryId()));
        context.setThreadLocalInfo();
        context.setStartTime();

        context.setCommand(MysqlCommand.COM_QUERY);

        return context;
    }

    /*

    private static List<TResultBatch> executeStmt(ConnectContext context, ExecPlan plan) throws Exception {
        Coordinator coord = new Coordinator(context, plan.getFragments(), plan.getScanNodes(), plan.getDescTbl().toThrift());
        coord.exec();

        RowBatch batch;
        List<TResultBatch> sqlResult = Lists.newArrayList();
        do {
            batch = coord.getNext();
            if (batch.getBatch() != null) {
                sqlResult.add(batch.getBatch());
            }
        } while (!batch.isEos());

        return sqlResult;
    }

    private static ExecPlan getExecutePlan(Map<String, Database> dbs, ConnectContext context,
                                           StatementBase parsedStmt, boolean isStatistic, boolean isLockDb) {
        ExecPlan execPlan;
        try {
            if (isLockDb) {
                lock(dbs);
            }

            Analyzer.analyze(parsedStmt, context);

            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, context).transform(
                    ((QueryStatement) parsedStmt).getQueryRelation());

            Optimizer optimizer = new Optimizer();
            OptExpression optimizedPlan = optimizer.optimize(
                    context,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);

            execPlan = new PlanFragmentBuilder()
                    .createStatisticPhysicalPlan(optimizedPlan, context, logicalPlan.getOutputColumn(),
                            columnRefFactory, isStatistic);
        } finally {
            if (isLockDb) {
                unLock(dbs);
            }
        }
        return execPlan;
    }

    private static List<TStatisticData> deserializerStatisticData(List<TResultBatch> sqlResult) throws TException {
        List<TStatisticData> statistics = Lists.newArrayList();

        if (sqlResult.size() < 1) {
            return statistics;
        }

        int version = sqlResult.get(0).getStatistic_version();
        if (sqlResult.stream().anyMatch(d -> d.getStatistic_version() != version)) {
            return statistics;
        }

        if (version == STATISTIC_DATA_VERSION || version == STATISTIC_DICT_VERSION) {
            TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
            for (TResultBatch resultBatch : sqlResult) {
                for (ByteBuffer bb : resultBatch.rows) {
                    TStatisticData sd = new TStatisticData();
                    byte[] bytes = new byte[bb.limit() - bb.position()];
                    bb.get(bytes);
                    deserializer.deserialize(sd, bytes);
                    statistics.add(sd);
                }
            }
        }

        return statistics;
    }

     */
}
