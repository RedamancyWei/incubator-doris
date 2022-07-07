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

public abstract class Packet {
    public final int HEADER_SIZE = 4;

    public final int MAX_PACKET_SIZE = 16 * 1024 * 1024;

    public int packetLength;

    public byte packetSequenceId;

    public abstract int calcPacketSize();

    public abstract String getPacketInfo();

    public void read(ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    public void write(ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }
}