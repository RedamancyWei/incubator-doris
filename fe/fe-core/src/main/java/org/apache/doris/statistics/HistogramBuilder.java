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

import org.apache.doris.catalog.Type;

import java.util.List;

public class HistogramBuilder {
    private Type dataType;

    private int maxBucketSize;
    private int bucketSize;
    private double sampleRate;

    private List<Bucket> buckets;

    public HistogramBuilder() {
    }

    public HistogramBuilder(Histogram histogram) {
        this.dataType = histogram.getDataType();
        this.maxBucketSize = histogram.getMaxBucketSize();
        this.bucketSize = histogram.getBucketSize();
        this.sampleRate = histogram.getSampleRate();
        this.buckets = histogram.getBuckets();
    }

    public HistogramBuilder setDataType(Type dataType) {
        this.dataType = dataType;
        return this;
    }

    public HistogramBuilder setMaxBucketSize(int maxBucketSize) {
        this.maxBucketSize = maxBucketSize;
        return this;
    }

    public HistogramBuilder setBucketSize(int bucketSize) {
        this.bucketSize = bucketSize;
        return this;
    }

    public HistogramBuilder setSampleRate(double sampleRate) {
        this.sampleRate = sampleRate;
        return this;
    }

    public HistogramBuilder setBuckets(List<Bucket> buckets) {
        this.buckets = buckets;
        return this;
    }

    public Type getDataType() {
        return dataType;
    }

    public int getMaxBucketSize() {
        return maxBucketSize;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public double getSampleRate() {
        return sampleRate;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public Histogram build() {
        Histogram histogram = new Histogram(dataType);
        histogram.setMaxBucketSize(maxBucketSize);
        histogram.setBucketSize(bucketSize);
        histogram.setSampleRate(sampleRate);
        histogram.setBuckets(buckets);
        return histogram;
    }
}
