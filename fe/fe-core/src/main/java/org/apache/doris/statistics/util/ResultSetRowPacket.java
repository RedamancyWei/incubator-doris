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

import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.List;

public class ResultSetRowPacket extends Packet {
    public List<Object> values = Lists.newArrayList();

    public long columnCount;

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    public long getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(long columnCount) {
        this.columnCount = columnCount;
    }

    @Override
    public void read(ByteBuffer buffer) {
        Message message = new Message(buffer);
        packetLength = message.readUB3();
        packetSequenceId = message.read();
        for (int i = 0; i < columnCount; i++) {
            values.add(message.readStringWithLength());
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
        return "MySQL QueryResultSet Row Packet";
    }

    @Override
    public String toString() {
        return "ResultSetRowPacket{"
                + " packetLength=" + packetLength
                + ", packetSequenceId=" + packetSequenceId
                + ", values=" + values
                + "}\n";
    }
}
