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

package org.apache.doris.load.routineload;

import com.google.common.collect.Lists;
import mockit.Deencapsulation;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KafkaRoutineLoadJobTest {

    @Test
    public void testBeNumMin(@Mocked KafkaConsumer kafkaConsumer,
                             @Injectable PartitionInfo partitionInfo1,
                             @Injectable PartitionInfo partitionInfo2,
                             @Mocked Catalog catalog,
                             @Mocked SystemInfoService systemInfoService,
                             @Mocked Database database) throws MetaNotFoundException {
        List<PartitionInfo> partitionInfoList = new ArrayList<>();
        partitionInfoList.add(partitionInfo1);
        partitionInfoList.add(partitionInfo2);
        List<Long> beIds = Lists.newArrayList(1L);

        String clusterName = "clusterA";

        new Expectations() {
            {
                kafkaConsumer.partitionsFor(anyString, (Duration) any);
                result = partitionInfoList;
                Catalog.getCurrentSystemInfo();
                result = systemInfoService;
                Catalog.getCurrentCatalog();
                result = catalog;
                catalog.getDb(anyLong);
                result = database;
                database.getClusterName();
                result = clusterName;
                systemInfoService.getClusterBackendIds(clusterName, true);
                result = beIds;
            }
        };

        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", "miaoling", 1L,
                1L, "1L", "v1", "", "", 3,
                RoutineLoadJob.JobState.NEED_SCHEDULER, RoutineLoadJob.DataSourceType.KAFKA, 0, new TResourceInfo(),
                "", "");
        Assert.assertEquals(1, kafkaRoutineLoadJob.calculateCurrentConcurrentTaskNum());
    }


    @Test
    public void testDivideRoutineLoadJob() {

        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, "kafka_routine_load_job", "miaoling", 1L,
                1L, "1L", "v1", "", "", 3,
                RoutineLoadJob.JobState.NEED_SCHEDULER, RoutineLoadJob.DataSourceType.KAFKA, 0, new TResourceInfo(),
                "", "");

        Deencapsulation.setField(kafkaRoutineLoadJob, "kafkaPartitions", Arrays.asList(1, 4, 6));

        List<RoutineLoadTask> result = kafkaRoutineLoadJob.divideRoutineLoadJob(2);
        Assert.assertEquals(2, result.size());
        for (RoutineLoadTask routineLoadTask : result) {
            KafkaRoutineLoadTask kafkaRoutineLoadTask = (KafkaRoutineLoadTask) routineLoadTask;
            if (kafkaRoutineLoadTask.getKafkaPartitions().size() == 2) {
                Assert.assertTrue(kafkaRoutineLoadTask.getKafkaPartitions().contains(1));
                Assert.assertTrue(kafkaRoutineLoadTask.getKafkaPartitions().contains(6));
            } else if (kafkaRoutineLoadTask.getKafkaPartitions().size() == 1) {
                Assert.assertTrue(kafkaRoutineLoadTask.getKafkaPartitions().contains(4));
            } else {
                Assert.fail();
            }
        }
    }
}
