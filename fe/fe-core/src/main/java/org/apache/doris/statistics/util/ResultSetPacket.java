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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ResultSetPacket extends Packet {
    public ColumnsNumberPacket columnsNumber;

    public List<ColumnPacket> columns = new ArrayList<>();

    public EofPacket columnsEof;

    public List<Packet> rows = new ArrayList<>();

    public EofPacket rowsEof;

    public int capabilities;

    public ColumnsNumberPacket getColumnsNumber() {
        return columnsNumber;
    }

    public void setColumnsNumber(ColumnsNumberPacket columnsNumber) {
        this.columnsNumber = columnsNumber;
    }

    public List<ColumnPacket> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnPacket> columns) {
        this.columns = columns;
    }

    public EofPacket getColumnsEof() {
        return columnsEof;
    }

    public void setColumnsEof(EofPacket columnsEof) {
        this.columnsEof = columnsEof;
    }

    public List<Packet> getRows() {
        return rows;
    }

    public void setRows(List<Packet> rows) {
        this.rows = rows;
    }

    public EofPacket getRowsEof() {
        return rowsEof;
    }

    public void setRowsEof(EofPacket rowsEof) {
        this.rowsEof = rowsEof;
    }

    public int getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(int capabilities) {
        this.capabilities = capabilities;
    }

    @Override
    public void read(ByteBuffer buffer) {
        columnsNumber = new ColumnsNumberPacket();
        columnsNumber.read(buffer);

        for (int i = 0; i < this.columnsNumber.columnsNumber; i++) {
            buffer = buffer.slice();
            ColumnPacket cp = new ColumnPacket();
            cp.read(buffer);
            columns.add(cp);
        }

        buffer = buffer.slice();
        if ((buffer.get(4) & 0xff) == 0xfe) {
            columnsEof = new EofPacket();
            columnsEof.read(buffer);
        }

        for (; ; ) {
            buffer = buffer.slice();
            int packetType = buffer.get(4) & 0xff;
            if (packetType == 0xfe) {
                rowsEof = new EofPacket();
                rowsEof.read(buffer);
                break;
            } else if (packetType == 0x00) {
                BinaryResultSetRowPacket brsrp = new BinaryResultSetRowPacket();
                brsrp.columns = columns;
                brsrp.read(buffer);
                rows.add(brsrp);
            } else {
                ResultSetRowPacket rsrp = new ResultSetRowPacket();
                rsrp.columnCount = columns.size();
                rsrp.read(buffer);
                rows.add(rsrp);
            }
        }
    }

    @Override
    public void write(ByteBuffer buffer) {
        super.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        return 0;
    }

    @Override
    public String getPacketInfo() {
        return null;
    }

    @Override
    public String toString() {
        return "ResultSetPacket{"
                + "columnsNumber=" + columnsNumber
                + ", columns=" + columns
                + ", columnsEof=" + columnsEof
                + ", rows=" + rows
                + ", rowsEof=" + rowsEof
                + ", capabilities=" + capabilities
                + '}';
    }
}
