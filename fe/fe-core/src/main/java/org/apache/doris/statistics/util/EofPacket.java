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

public class EofPacket extends Packet {
    public int header;

    public int warnings;

    public int statusFlags;

    public int capabilities;

    @Override
    public void read(ByteBuffer buffer) {
        Message message = new Message(buffer);
        packetLength = message.readUB3();
        packetSequenceId = message.read();
        header = message.read() & 0xff;

        if ((capabilities & MysqlCapability.Flag.CLIENT_PROTOCOL_41.getFlagBit()) != 0) {
            warnings = message.readUB2();
            statusFlags = message.readUB2();
        } else {
            message.move(4);
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
        return "sql eof packet";
    }

    @Override
    public String toString() {
        return "EofPacket{"
                + "header=" + header
                + ", warnings=" + warnings
                + ", statusFlags=" + statusFlags
                + ", capabilities=" + capabilities
                + '}';
    }
}

