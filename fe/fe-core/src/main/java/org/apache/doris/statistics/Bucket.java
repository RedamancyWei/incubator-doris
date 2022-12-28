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

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.collect.Lists;

import java.util.List;

public class Bucket {
    public LiteralExpr lower;
    public LiteralExpr upper;
    public int count;
    public int preSum;
    public int ndv;

    public LiteralExpr getLower() {
        return lower;
    }

    public void setLower(LiteralExpr lower) {
        this.lower = lower;
    }

    public LiteralExpr getUpper() {
        return upper;
    }

    public void setUpper(LiteralExpr upper) {
        this.upper = upper;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getPreSum() {
        return preSum;
    }

    public void setPreSum(int preSum) {
        this.preSum = preSum;
    }

    public int getNdv() {
        return ndv;
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }

    public static List<Bucket> deserializeFromjson(Type datatype, JSONArray jsonArray)
            throws AnalysisException {
        List<Bucket> buckets = Lists.newArrayList();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject bucketJson = jsonArray.getJSONObject(i);
            Bucket bucket = new Bucket();
            bucket.lower = StatisticsUtil.readableValue(datatype, bucketJson.get("lower").toString());
            bucket.upper = StatisticsUtil.readableValue(datatype, bucketJson.get("upper").toString());
            bucket.count = bucketJson.getIntValue("count");
            bucket.preSum = bucketJson.getIntValue("pre_sum");
            bucket.ndv = bucketJson.getIntValue("ndv");
            buckets.add(bucket);
        }
        return buckets;
    }
}
