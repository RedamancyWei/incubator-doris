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

public class BufferUtil {

    public static void writeUB2(ByteBuffer buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
    }

    public static void writeUB3(ByteBuffer buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
        buffer.put((byte) (i >>> 16));
    }

    public static void writeInt(ByteBuffer buffer, int i) {
        buffer.put((byte) (i & 0xff));
        buffer.put((byte) (i >>> 8));
        buffer.put((byte) (i >>> 16));
        buffer.put((byte) (i >>> 24));
    }

    public static void writeFloat(ByteBuffer buffer, float f) {
        writeInt(buffer, Float.floatToIntBits(f));
    }

    public static void writeLong(ByteBuffer buffer, long l) {
        buffer.put((byte) (l & 0xff));
        buffer.put((byte) (l >>> 8));
        buffer.put((byte) (l >>> 16));
        buffer.put((byte) (l >>> 24));
        buffer.put((byte) (l >>> 32));
        buffer.put((byte) (l >>> 40));
        buffer.put((byte) (l >>> 48));
        buffer.put((byte) (l >>> 56));
    }

    public static void writeUB4(ByteBuffer buffer, long l) {
        buffer.put((byte) (l & 0xff));
        buffer.put((byte) (l >>> 8));
        buffer.put((byte) (l >>> 16));
        buffer.put((byte) (l >>> 24));
    }

    public static void writeDouble(ByteBuffer buffer, double d) {
        writeLong(buffer, Double.doubleToLongBits(d));
    }

    public static void writeLength(ByteBuffer buffer, long l) {
        if (l < 0xfb) {
            buffer.put((byte) (l & 0xff));
        } else if (l < 0x10000) {
            buffer.put((byte) 0xfc);
            writeUB2(buffer, (int) l);
        } else if (l < 0x1000000) {
            buffer.put((byte) 0xfd);
            writeUB3(buffer, (int) l);
        } else {
            buffer.put((byte) 0xfe);
            writeLong(buffer, l);
        }
    }

    public static void writeWithNull(ByteBuffer buffer, byte[] src) {
        buffer.put(src);
        buffer.put((byte) 0);
    }

    public static void writeWithLength(ByteBuffer buffer, byte[] src) {
        long length = src.length;
        writeLength(buffer, length);
        buffer.put(src);
    }

    public static void writeWithLength(ByteBuffer buffer, byte[] src, byte nullValue) {
        if (src == null) {
            buffer.put(nullValue);
        } else {
            writeWithLength(buffer, src);
        }
    }

    public static int getLength(long length) {
        if (length < 251) {
            return 1;
        } else if (length < 0x10000) {
            return 3;
        } else if (length < 0x1000000) {
            return 4;
        } else {
            return 9;
        }
    }

    public static int getLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }
}
