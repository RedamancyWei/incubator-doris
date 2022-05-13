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

import java.util.List;

public class StatisticsDesc {
    private StatsCategory category;

    private StatsGranularity granularity;

    private List<StatsType> statsTypes;

    public StatisticsDesc(StatsCategory category,
                          StatsGranularity granularity,
                          List<StatsType> statsTypes) {
        this.category = category;
        this.granularity = granularity;
        this.statsTypes = statsTypes;
    }

    public StatsCategory getCategory() {
        return category;
    }

    public void setCategory(StatsCategory category) {
        this.category = category;
    }

    public StatsGranularity getGranularity() {
        return granularity;
    }

    public void setGranularity(StatsGranularity granularity) {
        this.granularity = granularity;
    }

    public List<StatsType> getStatsTypes() {
        return statsTypes;
    }

    public void setStatsTypes(List<StatsType> statsTypes) {
        this.statsTypes = statsTypes;
    }
}

