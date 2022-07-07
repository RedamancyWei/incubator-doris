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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryResultSet {
    private final ResultSetPacket packet;

    private Map<String, Integer> columnIndexMap;

    private int position = -1;

    public QueryResultSet(ResultSetPacket packet) {
        if (packet == null) {
            throw new IllegalArgumentException("packet is null");
        }
        this.packet = packet;
        packet.getPacketInfo();
    }

    public void buildColumnIndexMap() {
        List<ColumnPacket> columns = packet.getColumns();
        if (columns == null) {
            columnIndexMap = new HashMap<>(0);
            return;
        }

        columnIndexMap = new HashMap<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            columnIndexMap.put(columns.get(i).getName(), i);
        }
    }

    private int findColumn(String columnName) throws SQLException {
        if (columnIndexMap == null) {
            buildColumnIndexMap();
        }

        int index = columnIndexMap.getOrDefault(columnName, -1);
        if (index == -1) {
            throw new SQLException("column not found");
        }

        return index;
    }

    public Object getColumnValue(String columnName) throws SQLException {
        checkPosition(position);
        int index = findColumn(columnName);
        List<List<Object>> rows = getRows();
        if (rows == null) {
            return null;
        }
        return rows.get(position).get(index);
    }

    private void checkPosition(int position) {
        if (position >= packet.getRows().size()) {
            throw new IllegalArgumentException("position is out of range");
        }
    }

    public boolean hasRows() {
        if (packet == null) {
            return false;
        }
        return packet.getRows() != null && !packet.getRows().isEmpty();
    }

    public boolean next() throws SQLException {
        if (position >= packet.getRows().size() - 1) {
            position = -1;
            return false;
        }
        position++;
        return true;
    }

    public List<String> getColumns() {
        List<ColumnPacket> columns = packet.getColumns();
        if (columns == null) {
            return null;
        }
        return columns.stream().map(ColumnPacket::getName).collect(Collectors.toList());
    }

    public List<List<Object>> getRows() {
        if (hasRows()) {
            List<Packet> rows = packet.getRows();
            if (rows == null) {
                return null;
            }

            List<List<Object>> result = new ArrayList<>();
            for (Packet row : rows) {
                ResultSetRowPacket rowPacket = (ResultSetRowPacket) row;
                result.add(rowPacket.getValues());
            }

            return result;
        }

        return null;
    }
}
