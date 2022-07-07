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

public class ErrPacket extends Packet {
    public int header;

    public int errorCode;

    public byte sqlStateMarker;

    public String sqlState;

    public String errorMessage;

    public int capabilities;

    @Override
    public void read(ByteBuffer buffer) {
        Message message = new Message(buffer);
        packetLength = message.readUB3();
        packetSequenceId = message.read();
        header = message.read();
        errorCode = message.readUB2();

        if ((capabilities & MysqlCapability.Flag.CLIENT_PROTOCOL_41.getFlagBit()) > 0) {
            sqlStateMarker = message.read();
            sqlState = new String(message.readBytes(5));
        }
        errorMessage = message.readString();
    }

    @Override
    public void write(ByteBuffer buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetSequenceId);
        buffer.put((byte) 0xff);
        BufferUtil.writeUB2(buffer, errorCode);
        if ((capabilities & MysqlCapability.Flag.CLIENT_PROTOCOL_41.getFlagBit()) > 0) {
            buffer.put((byte) '#');
            buffer.put(sqlState.getBytes());
        }
        if (errorMessage != null) {
            buffer.put(errorMessage.getBytes());
        }
    }

    @Override
    public int calcPacketSize() {
        int size = 9;
        if (errorMessage != null) {
            size += errorMessage.length();
        }
        return size;
    }

    @Override
    public String getPacketInfo() {
        return "MySQL Error Packet";
    }

    @Override
    public String toString() {
        return "ErrPacket{"
                + "header=" + header
                + ", errorCode=" + errorCode
                + ", sqlStateMarker=" + sqlStateMarker
                + ", sqlState='" + sqlState + '\''
                + ", errorMessage='" + errorMessage + '\''
                + ", capabilities=" + capabilities
                + '}';
    }
}

