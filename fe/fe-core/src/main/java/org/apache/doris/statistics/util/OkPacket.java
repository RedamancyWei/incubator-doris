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

public class OkPacket extends Packet {
    public byte header;

    public long affectedRows;

    public long lastInsertId;

    public int statusFlags;

    public int warnings;

    public String info;

    public int capabilities;

    public String sessionStateChanges;

    @Override
    public void read(ByteBuffer buffer) {
        Message message = new Message(buffer);
        packetLength = message.readUB3();
        packetSequenceId = message.read();
        header = message.read();
        affectedRows = message.readLength();
        lastInsertId = message.readLength();

        if ((capabilities & MysqlCapability.Flag.CLIENT_PROTOCOL_41.getFlagBit()) > 0) {
            statusFlags = message.readUB2();
            warnings = message.readUB2();
        } else if ((capabilities & MysqlCapability.Flag.CLIENT_TRANSACTIONS.getFlagBit()) > 0) {
            statusFlags = message.readUB2();
        }

        if ((capabilities & MysqlCapability.Flag.CLIENT_SESSION_TRACK.getFlagBit()) > 0) {
            info = message.readStringWithLength();
            if ((statusFlags & StatusFlags.SERVER_SESSION_STATE_CHANGED.getCode()) > 0) {
                sessionStateChanges = message.readStringWithLength();
            }
        } else {
            info = new String(message.readBytes(packetLength + 4 - buffer.position()));
        }
    }

    @Override
    public void write(ByteBuffer buffer) {
        packetLength = calcPacketSize();
        BufferUtil.writeUB3(buffer, packetLength);

        buffer.put(packetSequenceId);
        buffer.put((byte) 0x00);
        BufferUtil.writeLength(buffer, affectedRows);
        BufferUtil.writeLength(buffer, lastInsertId);

        if ((capabilities & MysqlCapability.Flag.CLIENT_PROTOCOL_41.getFlagBit()) != 0) {
            BufferUtil.writeUB2(buffer, statusFlags);
            BufferUtil.writeUB2(buffer, warnings);
        } else if ((capabilities & MysqlCapability.Flag.CLIENT_TRANSACTIONS.getFlagBit()) != 0) {
            BufferUtil.writeUB2(buffer, statusFlags);
        }

        if ((capabilities & MysqlCapability.Flag.CLIENT_SESSION_TRACK.getFlagBit()) != 0) {
            BufferUtil.writeWithLength(buffer, info.getBytes());
            if ((statusFlags & StatusFlags.SERVER_SESSION_STATE_CHANGED.getCode()) != 0) {
                BufferUtil.writeWithLength(buffer, sessionStateChanges.getBytes());
            }
        } else {
            if (info != null) {
                buffer.put(info.getBytes());
            }
        }
    }

    @Override
    public int calcPacketSize() {
        int size = 1 + BufferUtil.getLength(affectedRows) + BufferUtil.getLength(lastInsertId);

        if ((capabilities & MysqlCapability.Flag.CLIENT_PROTOCOL_41.getFlagBit()) != 0) {
            size += 4;
        } else if ((capabilities & MysqlCapability.Flag.CLIENT_TRANSACTIONS.getFlagBit()) != 0) {
            size += 2;
        }

        if ((capabilities & MysqlCapability.Flag.CLIENT_SESSION_TRACK.getFlagBit()) != 0) {
            size += BufferUtil.getLength(info.getBytes());
            if ((statusFlags & StatusFlags.SERVER_SESSION_STATE_CHANGED.getCode()) != 0) {
                size += BufferUtil.getLength(sessionStateChanges.getBytes());
            }
        } else {
            if (info != null) {
                size += info.getBytes().length;
            }
        }

        return size;
    }

    @Override
    public String getPacketInfo() {
        return "Sql ok packet";
    }

    @Override
    public String toString() {
        return "OkPacket{" + "header=" + header + ", affectedRows=" + affectedRows + ", lastInsertId=" + lastInsertId
                + ", statusFlags=" + statusFlags + ", warnings=" + warnings + ", info='" + info + '\''
                + ", sessionStateChanges='" + sessionStateChanges + '\'' + ", capabilities=" + capabilities
                + ", HEADER_SIZE=" + HEADER_SIZE + ", MAX_PACKET_SIZE=" + MAX_PACKET_SIZE + ", packetLength="
                + packetLength + ", packetSequenceId=" + packetSequenceId + '}';
    }
}
