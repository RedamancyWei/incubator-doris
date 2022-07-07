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

public class HandshakePacket extends Packet {
    public static final byte[] RESERVED_FILL = new byte[10];

    public int protocolVersion;

    public String serverVersion;

    public int connectionId;

    public byte[] authPluginDataPart1;

    public byte filler;

    public int capabilityLower;

    public byte characterSet;

    public int statusFlags;

    public int capabilityUpper;

    public byte authPluginDataLen;

    public byte[] reserved;

    public byte[] authPluginDataPart2;

    public String authPluginName;

    public int capabilities;

    public HandshakePacket() {
    }

    @Override
    public void read(ByteBuffer buffer) {
        Message message = new Message(buffer);
        packetLength = message.readUB3();
        packetSequenceId = message.read();
        protocolVersion = message.read();
        serverVersion = message.readStringWithNull();
        connectionId = message.readUB4();
        authPluginDataPart1 = message.readBytes(8);
        filler = message.read();
        capabilityLower = message.readUB2();
        characterSet = (byte) (message.read() & 0xff);
        statusFlags = message.readUB2();
        capabilityUpper = message.readUB2();
        capabilities = capabilityUpper << 16 | capabilityLower;
        authPluginDataLen = 0;

        if ((capabilities & MysqlCapability.Flag.CLIENT_PLUGIN_AUTH.getFlagBit()) > 0) {
            authPluginDataLen = message.read();
        } else {
            message.move(1);
        }

        reserved = message.readBytes(10);

        if ((capabilities & MysqlCapability.Flag.CLIENT_SECURE_CONNECTION.getFlagBit()) > 0) {
            authPluginDataPart2 = message.readBytesWithNull();
        } else {
            message.move(13);
        }

        authPluginName = message.readStringWithNull();
    }

    @Override
    public void write(ByteBuffer buffer) {
        BufferUtil.writeUB3(buffer, packetLength);
        buffer.put(packetSequenceId);
        buffer.put((byte) protocolVersion);
        BufferUtil.writeWithNull(buffer, serverVersion.getBytes());
        BufferUtil.writeUB4(buffer, connectionId);
        buffer.put(authPluginDataPart1);
        buffer.put((byte) 0x00);
        BufferUtil.writeUB2(buffer, capabilityLower);
        buffer.put(characterSet);
        BufferUtil.writeUB2(buffer, statusFlags);
        BufferUtil.writeUB2(buffer, capabilityUpper);
        authPluginDataLen = 21;
        buffer.put(authPluginDataLen);
        buffer.put(RESERVED_FILL);
        buffer.put(authPluginDataPart2);
        buffer.put((byte) 0x00);
        BufferUtil.writeWithNull(buffer, authPluginName.getBytes());
    }

    @Override
    public int calcPacketSize() {
        return 45 + 2 + serverVersion.length() + authPluginName.length();
    }

    @Override
    public String getPacketInfo() {
        return "Sql handshake packet";
    }

    @Override
    public String toString() {
        return "HandshakePacket{"
                + "protocolVersion=" + protocolVersion
                + ", serverVersion='" + serverVersion + '\''
                + ", connectionId=" + connectionId
                + ", authPluginDataPart1=" + Arrays.toString(authPluginDataPart1)
                + ", filler=" + filler
                + ", capabilityLower=" + capabilityLower
                + ", characterSet=" + characterSet
                + ", statusFlags=" + statusFlags
                + ", capabilityUpper=" + capabilityUpper
                + ", authPluginDataLen=" + authPluginDataLen
                + ", reserved=" + Arrays.toString(reserved)
                + ", authPluginDataPart2=" + Arrays.toString(authPluginDataPart2)
                + ", authPluginName='" + authPluginName + '\''
                + ", capabilities=" + capabilities
                + '}';
    }
}

