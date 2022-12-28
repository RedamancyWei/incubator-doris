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
import org.apache.doris.catalog.Type;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.util.List;

public class Histogram {
    private static final Logger LOG = LogManager.getLogger(Histogram.class);

    private Type dataType;

    private int maxBucketSize;
    private int bucketSize;
    private double sampleRate;

    private List<Bucket> buckets;

    public Histogram(Type dataType) {
        this.dataType = dataType;
    }

    public Type getDataType() {
        return dataType;
    }

    public void setDataType(Type dataType) {
        this.dataType = dataType;
    }

    public int getMaxBucketSize() {
        return maxBucketSize;
    }

    public void setMaxBucketSize(int maxBucketSize) {
        this.maxBucketSize = maxBucketSize;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public void setBucketSize(int bucketSize) {
        this.bucketSize = bucketSize;
    }

    public double getSampleRate() {
        return sampleRate;
    }

    public void setSampleRate(double sampleRate) {
        if (sampleRate < 0 || sampleRate > 1.0) {
            this.sampleRate = 1.0;
        } else {
            this.sampleRate = sampleRate;
        }
    }

    public void setBuckets(List<Bucket> buckets) {
        this.buckets = buckets;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public static Histogram DEFAULT = new HistogramBuilder().setMaxBucketSize(1)
            .setBucketSize(0).setSampleRate(1.0).setBuckets(Lists.newArrayList()).build();

    // TODO: use thrift
    public static Histogram fromResultRow(ResultRow resultRow) {
        try {
            HistogramBuilder histogramBuilder = new HistogramBuilder();

            long catalogId = Long.parseLong(resultRow.getColumnValue("catalog_id"));
            long idxId = Long.parseLong(resultRow.getColumnValue("idx_id"));
            long dbId = Long.parseLong(resultRow.getColumnValue("db_id"));
            long tblId = Long.parseLong(resultRow.getColumnValue("tbl_id"));

            String colName = resultRow.getColumnValue("col_id");
            Column col = StatisticsUtil.findColumn(catalogId, dbId, tblId, idxId, colName);
            if (col == null) {
                LOG.warn("Failed to deserialize histogram statistics, ctlId: {} dbId: {}"
                                + "tblId: {} column: {} not exists",
                        catalogId, dbId, tblId, colName);
                return Histogram.DEFAULT;
            }

            double sampleRate = Double.parseDouble(resultRow.getColumnValue("sample_rate"));
            histogramBuilder.setSampleRate(sampleRate);
            histogramBuilder.setDataType(col.getType());

            String json = resultRow.getColumnValue("buckets");
            JSONObject jsonObj = JSON.parseObject(json);
            JSONArray jsonArray = jsonObj.getJSONArray("buckets");
            List<Bucket> buckets = Bucket.deserializeFromjson(col.getType(), jsonArray);
            histogramBuilder.setBuckets(buckets);
            histogramBuilder.setBucketSize(buckets.size());

            return histogramBuilder.build();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("Failed to deserialize histogram statistics, column not exists", e);
            return Histogram.DEFAULT;
        }
    }

    /**
     * Histogram info is stored in an internal table in json format,
     * and Histogram obj can be obtained by this method.
     */
    public static Histogram deserializeFromJson(Type datatype, String json) {
        if (Strings.isNullOrEmpty(json)) {
            return null;
        }

        try {
            Histogram histogram = new Histogram(datatype);
            JSONObject histogramJson = JSON.parseObject(json);
            JSONArray jsonArray = histogramJson.getJSONArray("buckets");

            List<Bucket> buckets = Bucket.deserializeFromjson(datatype, jsonArray);
            histogram.setBuckets(buckets);

            int maxBucketSize = histogramJson.getIntValue("max_bucket_size");
            histogram.setMaxBucketSize(maxBucketSize);

            int bucketSize = histogramJson.getIntValue("bucket_size");
            histogram.setBucketSize(bucketSize);

            double sampleRate = histogramJson.getDoubleValue("sample_rate");
            histogram.setSampleRate(sampleRate);

            return histogram;
        } catch (Throwable e) {
            LOG.warn("deserialize from json error, input json string: {}", json, e);
        }

        return null;
    }

    /**
     * Convert to json format string
     */
    public static String serializeToJson(Histogram histogram) {
        if (histogram == null) {
            return "";
        }

        JSONObject histogramJson = new JSONObject();
        histogramJson.put("max_bucket_size", histogram.maxBucketSize);
        histogramJson.put("bucket_size", histogram.bucketSize);
        histogramJson.put("sample_rate", histogram.sampleRate);

        JSONArray bucketsJsonArray = new JSONArray();
        histogramJson.put("buckets", bucketsJsonArray);

        if (histogram.buckets != null) {
            for (Bucket bucket : histogram.buckets) {
                JSONObject bucketJson = new JSONObject();
                bucketJson.put("count", bucket.count);
                bucketJson.put("pre_sum", bucket.preSum);
                bucketJson.put("ndv", bucket.ndv);
                bucketJson.put("upper", bucket.upper.getStringValue());
                bucketJson.put("lower", bucket.lower.getStringValue());
                bucketsJsonArray.add(bucketJson);
            }
        }

        return histogramJson.toJSONString();
    }
}
