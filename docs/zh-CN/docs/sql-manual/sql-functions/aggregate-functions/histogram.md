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

histogram(直方图)函数用于描述数据分布情况，它使用“等高”的分桶策略，并按照数据的值大小进行分桶，并用一些简单的数据来描述每个桶，比如落在桶里的值的个数。主要用于优化器进行区间查询的估算。

有两个可选的参数：

- sample_rate：该值用来设置抽样的比例，默认值为0.2。

- max_bucket_size：该值用来设置最大的分桶数量，默认值为128。

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
查询结果说明：

>



### keywords

HISTOGRAM
