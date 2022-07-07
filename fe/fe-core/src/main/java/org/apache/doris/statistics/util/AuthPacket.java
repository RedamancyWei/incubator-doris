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

import org.apache.doris.mysql.MysqlCapability;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class AuthPacket extends Packet {
    private static final byte[] FILLER = new byte[23];

    public int capabilityFlags;

    public int maxPacket = 0x1 << 24 - 1;

    public byte characterSet;

    public byte[] reserved;

    public String username;

    public byte[] password;

    public int authResponseLength;

    public String database;

    public String authPluginName;

    public String keyValuesLength;

    public Map<String, String> values;

    @Override
    public void read(ByteBuffer buffer) {
        Message message = new Message(buffer);
        packetLength = message.readUB3();
        packetSequenceId = message.read();
        capabilityFlags = message.readUB4();
        maxPacket = message.readUB4();
        characterSet = message.read();
        message.move(23);
        username = message.readStringWithNull();

        if ((capabilityFlags & MysqlCapability.Flag.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.getFlagBit()) != 0) {
            password = message.readBytesWithLength();
        } else if ((capabilityFlags & MysqlCapability.Flag.CLIENT_SECURE_CONNECTION.getFlagBit()) != 0) {
            authResponseLength = message.read();
            password = message.readBytes(authResponseLength);
        } else {
            message.move(1);
        }

        if ((capabilityFlags & MysqlCapability.Flag.CLIENT_CONNECT_WITH_DB.getFlagBit()) != 0) {
            database = message.readStringWithNull();
        }

        if ((capabilityFlags & MysqlCapability.Flag.CLIENT_PLUGIN_AUTH.getFlagBit()) != 0) {
            authPluginName = message.readStringWithNull();
        }
    }

    @Override
    public void write(ByteBuffer buffer) {
        packetLength = calcPacketSize();
        BufferUtil.writeUB3(buffer, packetLength);
        buffer.put(packetSequenceId);
        BufferUtil.writeUB4(buffer, capabilityFlags);
        BufferUtil.writeInt(buffer, maxPacket);
        buffer.put(characterSet);
        reserved = FILLER;
        buffer.put(FILLER);

        if (username == null) {
            buffer.put((byte) 0);
        } else {
            BufferUtil.writeWithNull(buffer, username.getBytes());
        }

        if (password == null) {
            buffer.put((byte) 0);
        } else {
            BufferUtil.writeWithLength(buffer, password);
        }

        if (database == null) {
            buffer.put((byte) 0);
        } else {
            BufferUtil.writeWithNull(buffer, database.getBytes());
        }

        if (authPluginName == null) {
            buffer.put((byte) 0);
        } else {
            BufferUtil.writeWithNull(buffer, authPluginName.getBytes());
        }

        buffer.limit(buffer.position());
    }

    @Override
    public int calcPacketSize() {
        int size = 9 + 23;

        if (username != null) {
            size += username.length() + 1;
        } else {
            size += 1;
        }

        if (password != null) {
            size += BufferUtil.getLength(password);
        } else {
            size += 1;
        }

        if (database != null) {
            size += database.length() + 1;
        } else {
            size += 1;
        }

        if (authPluginName != null) {
            size += authPluginName.length() + 1;
        } else {
            size += 1;
        }

        return size;
    }

    @Override
    public String getPacketInfo() {
        return "Sql authentication packet";
    }

    @Override
    public String toString() {
        return "AuthPacket{"
                + "capabilityFlags=" + capabilityFlags
                + ", maxPacket=" + maxPacket
                + ", characterSet=" + characterSet
                + ", reserved=" + Arrays.toString(reserved)
                + ", username='" + username + '\''
                + ", password=" + Arrays.toString(password)
                + ", authResponseLength=" + authResponseLength
                + ", database='" + database + '\''
                + ", authPluginName='" + authPluginName + '\''
                + ", keyValuesLength='" + keyValuesLength + '\''
                + ", values=" + values
                + '}';
    }
}

