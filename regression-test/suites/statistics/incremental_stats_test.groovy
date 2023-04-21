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

suite("test_incremental_stats") {
    def dbName = "test_incremental_stats"
    def tblName = "${dbName}.example_tbl"
    
    def colStatisticsTblName = "__internal_schema.column_statistics"
    def analysisJobsTblName = "__internal_schema.analysis_jobs"
    
    def query_col_statistics_with_order_sql = """
        SELECT 
            count, 
            ndv, 
            null_count, 
            min, 
            max, 
            data_size_in_bytes 
        FROM 
            ${colStatisticsTblName} 
        WHERE 
            col_id in (
              'user_id', 'date', 'city', 
              'age', 'sex', 'last_visit_date', 
              'cost', 'max_dwell_time', 'min_dwell_time'
            ) 
        ORDER BY 
            part_id,
            user_id;
        """

    def query_analysis_jobs_with_order_sql = """
        SELECT 
            catalog_name,
            db_name,
            tbl_name,
            col_name,
            index_id,
            job_type,
            analysis_type,
            message,
            state,
            schedule_type
        FROM 
            ${analysisJobsTblName} 
        WHERE 
            db_name = '${dbName}'
            AND tbl_name = '${tblName}'
            AND col_name IN (
              'user_id', 'date', 'city', 
              'age', 'sex', 'last_visit_date', 
              'cost', 'max_dwell_time', 'min_dwell_time'
            ) 
        ORDER BY 
            index_id, col_name;
        """

    sql "DROP DATABASE IF EXISTS ${dbName}"

    sql "CREATE DATABASE IF NOT EXISTS ${dbName};"

    sql "DROP TABLE IF EXISTS ${tblName}"

    sql """
        CREATE TABLE IF NOT EXISTS ${tblName} (
            `user_id` LARGEINT NOT NULL,
            `date` DATE NOT NULL,
            `city` VARCHAR(20),
            `age` SMALLINT,
            `sex` TINYINT,
            `last_visit_date` DATETIME REPLACE,
            `cost` BIGINT SUM,
            `max_dwell_time` INT MAX,
            `min_dwell_time` INT MIN
        ) ENGINE=OLAP
        AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
        PARTITION BY LIST(`date`)
        (
            PARTITION `p_201701` VALUES IN ("2017-10-01"),
            PARTITION `p_201702` VALUES IN ("2017-10-02"),
            PARTITION `p_201703` VALUES IN ("2017-10-03"),
            PARTITION `default`
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO ${tblName} (`user_id`, `date`, `city`, `age`,
                                            `sex`, `last_visit_date`, `cost`,
                                            `max_dwell_time`, `min_dwell_time`)
        VALUES (10000, "2017-10-01", "北京", 20, 0, "2017-10-01 07:00:00", 15, 2, 2),
            (10000, "2017-10-01", "北京", 20, 0, "2017-10-01 06:00:00", 20, 10, 10),
            (10001, "2017-10-01", "北京", 30, 1, "2017-10-01 17:05:45", 2, 22, 22),
            (10002, "2017-10-02", "上海", 20, 1, "2017-10-02 12:59:12", 200, 5, 5),
            (10003, "2017-10-02", "广州", 32, 0, "2017-10-02 11:20:00", 30, 11, 11),
            (10004, "2017-10-01", "深圳", 35, 0, "2017-10-01 10:00:15", 100, 3, 3),
            (10004, "2017-10-03", "深圳", 35, 0, "2017-10-03 10:20:22", 11, 6, 6);
    """

    // Firstly do a full collection of statistics
    sql "ANALYZE TABLE ${tblName} WITH sync;"

    qt_sql query_col_statistics_with_order_sql

    // Incrementally collect statistics
    sql "ANALYZE TABLE ${tblName} WITH sync WITH incremental;"

    // The table data has not changed, and no new tasks should be generated
    qt_sql query_analysis_jobs_with_order_sql

    // Statistics won't change either
    qt_sql query_col_statistics_with_order_sql

    // Drop a partition, then re-collect statistics
    sql "ALTER TABLE ${tblName} DROP PARTITION `p_201701`;"

    // Incrementally collect statistics
    sql "ANALYZE TABLE ${tblName} WITH sync WITH incremental;"

    // Although the partition is deleted, no new partition collection task will be generated,
    // but tasks to refresh table-level statistics will be generated
    qt_sql query_analysis_jobs_with_order_sql

    // Statistics will change either
    qt_sql query_col_statistics_with_order_sql

    // Add a partition, then re-collect statistics
    sql "ALTER TABLE ${tblName} ADD PARTITION `p_201701` VALUES IN ('2017-10-01');"

    sql """
        INSERT INTO ${tblName} (`user_id`, `date`, `city`, `age`,
                                            `sex`, `last_visit_date`, `cost`,
                                            `max_dwell_time`, `min_dwell_time`)
        VALUES (10000, "2017-10-01", "北京", 20, 0, "2017-10-01 07:00:00", 15, 2, 2),
            (10000, "2017-10-01", "北京", 20, 0, "2017-10-01 06:00:00", 20, 10, 10),
            (10001, "2017-10-01", "北京", 30, 1, "2017-10-01 17:05:45", 2, 22, 22),
            (10004, "2017-10-01", "深圳", 35, 0, "2017-10-01 10:00:15", 100, 3, 3);
    """

    // Incrementally collect statistics
    sql "ANALYZE TABLE ${tblName} WITH sync WITH incremental;"

    // Adding a new partition will generate new tasks to incrementally
    // collect the corresponding partition information,
    // and also have tasks update table-level statistical information
    qt_sql query_analysis_jobs_with_order_sql

    // Statistics will change either
    qt_sql query_col_statistics_with_order_sql

    // Finally, collect statistics in full
    sql "ANALYZE TABLE ${tblName} WITH sync;"

    // Will generate tasks to collect all partition statistics and update table statistics
    qt_sql query_analysis_jobs_with_order_sql

    // Compare statistics again
    qt_sql query_col_statistics_with_order_sql

    sql "DROP DATABASE IF EXISTS ${dbName}"
}
