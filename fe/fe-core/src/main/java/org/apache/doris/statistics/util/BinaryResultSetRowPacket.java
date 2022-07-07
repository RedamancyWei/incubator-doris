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
import java.util.Arrays;
import java.util.List;

public class BinaryResultSetRowPacket extends Packet {
    public int header;

    public byte[] nullBitmap;

    public List<ColumnPacket> columns;

    public List<Object> values = Lists.newArrayList();

    @Override
    public void read(ByteBuffer buffer) {
        Message message = new Message(buffer);
        packetLength = message.readUB3();
        packetSequenceId = message.read();
        header = message.read();

        if (columns != null && columns.size() > 0) {
            int size = (columns.size() + 9) / 8;
            nullBitmap = message.readBytes(size);
        }

        if (columns != null && columns.size() > 0) {
            for (int i = 0; i < columns.size(); i++) {
                if ((nullBitmap[(i + 2) / 8] & (1 << ((i + 2) % 8))) > 0) {
                    values.add("null");
                } else {
                    ColumnPacket cp = columns.get(i);
                    values.add(MySqlTypes.data(cp.type, message));
                }
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
        return "Sql binary resultSet row packet";
    }

    @Override
    public String toString() {
        return "BinaryResultSetRowPacket{"
                + "header=" + header
                + ", nullBitmap=" + Arrays.toString(nullBitmap)
                + ", columns=" + columns
                + ", values=" + values
                + '}';
    }
}
