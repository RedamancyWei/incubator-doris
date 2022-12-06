---
{
"title": "TOPN",
"language": "zh-CN"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## HISTOGRAM
### description
#### Syntax

`histogram(expr[, DOUBLE sample_rate][, INT max_bucket_size])`

The histogram function is used to describe the distribution of the data. It uses an "equal height" bucking strategy, and divides the data into buckets according to the value of the data. It describes each bucket with some simple data, such as the number of values that fall in the bucket. It is mainly used by the optimizer to estimate the range query.

There are two optional parameters：

- sample_rate：This value is used to set the sampling ratio, the default value is 0.2。

- max_bucket_size：This value is used to set the maximum number of buckets, the default value is 128。

### example

```
MySQL [test]> select histogram(imp_time) from dev_table;
+------------------------------------------------------------------------------------------------------------+
| histogram(`imp_time`)                                                                                       |
+------------------------------------------------------------------------------------------------------------+
| a:157, b:138, c:133, d:133, e:131, f:127, g:124, h:122, i:117, k:117                                       |
+------------------------------------------------------------------------------------------------------------+

MySQL [test]> select histogram(imp_time) from dev_table;;
+------------+-----------------------------------------------------------------------------------------------+
| date       | topn(`keyword`, 10, 100)                                                                      |
+------------+-----------------------------------------------------------------------------------------------+
| 2020-06-19 | a:11, b:8, c:8, d:7, e:7, f:7, g:7, h:7, i:7, j:7                                             |
| 2020-06-18 | a:10, b:8, c:7, f:7, g:7, i:7, k:7, l:7, m:6, d:6                                             |
| 2020-06-17 | a:9, b:8, c:8, j:8, d:7, e:7, f:7, h:7, i:7, k:7                                              |
+------------+-----------------------------------------------------------------------------------------------+
```
Query result description：

>



### keywords

HISTOGRAM
