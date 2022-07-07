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

public class ColumnPacket extends Packet {
    public String catalog;

    public String schema;

    public String table;

    public String orgTable;

    public String name;

    public String orgName;

    public int characterSet;

    public int columnLength;

    public int type;

    public int flags;

    public byte decimals;

    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getOrgTable() {
        return orgTable;
    }

    public String getName() {
        return name;
    }

    public String getOrgName() {
        return orgName;
    }

    public int getCharacterSet() {
        return characterSet;
    }

    public int getColumnLength() {
        return columnLength;
    }

    public int getType() {
        return type;
    }

    public int getFlags() {
        return flags;
    }

    public byte getDecimals() {
        return decimals;
    }

    @Override
    public void read(ByteBuffer buffer) {
        Message message = new Message(buffer);
        packetLength = message.readUB3();
        packetSequenceId = message.read();
        catalog = message.readStringWithLength();
        schema = message.readStringWithLength();
        table = message.readStringWithLength();
        orgTable = message.readStringWithLength();
        name = message.readStringWithLength();
        orgName = message.readStringWithLength();
        message.move(1);
        characterSet = message.readUB2();
        columnLength = message.readUB4();
        type = (message.read() & 0xff);
        flags = message.readUB2();
        decimals = message.read();
        message.move(2);
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
        return "Sql column packet";
    }

    @Override
    public String toString() {
        return "ColumnPacket{"
                + "catalog='" + catalog + '\''
                + ", schema='" + schema + '\''
                + ", table='" + table + '\''
                + ", orgTable='" + orgTable + '\''
                + ", name='" + name + '\''
                + ", orgName='" + orgName + '\''
                + ", characterSet=" + characterSet
                + ", columnLength=" + columnLength
                + ", type=" + type
                + ", flags=" + flags
                + ", decimals=" + decimals
                + '}';
    }
}

