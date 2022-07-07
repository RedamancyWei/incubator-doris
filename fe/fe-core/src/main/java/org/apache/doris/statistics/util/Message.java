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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

public class Message {
    public static final long NULL_LENGTH = -1;

    private static final byte[] EMPTY_BYTES = new byte[0];

    private static final ThreadLocal<Calendar> localCalendar = new ThreadLocal<Calendar>();

    private final ByteBuffer buffer;

    private final int length;

    public Message(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        buffer = byteBuffer;
        length = buffer.capacity();
    }

    private static Calendar getLocalCalendar() {
        Calendar cal = localCalendar.get();
        if (cal == null) {
            cal = Calendar.getInstance();
            localCalendar.set(cal);
        }
        return cal;
    }

    public byte[] data() {
        return buffer.array();
    }

    public int length() {
        return buffer.capacity();
    }

    public int position() {
        return buffer.position();
    }

    public void move(int i) {
        buffer.position(position() + i);
    }

    public void position(int i) {
        buffer.position(i);
    }

    public boolean hasRemaining() {
        return buffer.hasRemaining();
    }

    public byte read() {
        return buffer.get();
    }

    public byte read(int i) {
        return buffer.get(i);
    }

    public int readUB2() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        return i;
    }

    public int readUB3() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        i |= (read() & 0xff) << 16;
        return i;
    }

    public int readUB4() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        i |= (read() & 0xff) << 16;
        i |= (read() & 0xff) << 24;
        return i;
    }

    public int readInt() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        i |= (read() & 0xff) << 16;
        i |= (read() & 0xff) << 24;
        return i;
    }

    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    public long readLong() {
        long i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        i |= (read() & 0xff) << 16;
        i |= (read() & 0xff) << 24;
        i |= (read() & 0xff) << 32;
        i |= (read() & 0xff) << 40;
        i |= (read() & 0xff) << 48;
        i |= (read() & 0xff) << 56;
        return i;
    }

    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    public long readLength() {
        int length = read() & 0xff;
        switch (length) {
            case 251:
                return NULL_LENGTH;
            case 252:
                return readUB2();
            case 253:
                return readUB3();
            case 254:
                return readLong();
            default:
                return length;
        }
    }

    public byte[] readBytes() {
        int length = buffer.remaining();
        if (length <= 0) {
            return EMPTY_BYTES;
        }
        return readBytes(length);
    }

    public byte[] readBytes(int length) {
        byte[] ba = new byte[length];
        buffer.get(ba);
        return ba;
    }

    public byte[] readBytesWithNull() {
        int length = buffer.remaining();

        if (length <= 0) {
            return EMPTY_BYTES;
        }

        int offset = -1;
        int position = position();
        int limit = buffer.limit();

        for (int i = position; i < limit; i++) {
            if (buffer.get(i) == 0) {
                offset = i;
                break;
            }
        }

        switch (offset) {
            case -1:
                byte[] ba1 = new byte[length];
                buffer.get(ba1);
                return ba1;
            case 0:
                buffer.position(position + 1);
                return EMPTY_BYTES;
            default:
                byte[] ba2 = new byte[offset - position];
                buffer.get(ba2);
                buffer.get();
                return ba2;
        }
    }

    public byte[] readBytesWithLength() {
        int length = (int) readLength();
        if (length == NULL_LENGTH) {
            return null;
        }
        if (length <= 0) {
            return EMPTY_BYTES;
        }
        byte[] ba = new byte[length];
        buffer.get(ba);
        return ba;
    }

    public String readString() {
        int remain = buffer.remaining();
        if (remain == 0) {
            return null;
        }
        return new String(readBytes());
    }

    public String readString(String charset) throws UnsupportedEncodingException {
        int remain = buffer.remaining();
        if (remain == 0) {
            return null;
        }
        return new String(readBytes(), charset);

    }

    public String readStringWithNull() {
        byte[] bytes = readBytesWithNull();
        if (bytes == null) {
            return null;
        }
        return new String(bytes);
    }

    public String readStringWithNull(String charset) throws UnsupportedEncodingException {
        byte[] readed = readBytesWithNull();
        return (EMPTY_BYTES == readed) ? null : new String(readed, charset);
    }

    public String readStringWithLength() {
        byte[] ba = readBytesWithLength();
        if (ba != null) {
            return new String(ba);
        }
        return null;
    }

    public String readStringWithLength(String charset) throws UnsupportedEncodingException {
        byte[] ba = readBytesWithLength();
        if (ba != null) {
            return new String(ba, charset);
        }
        return null;
    }

    public Time readTime() {
        move(6);
        int hour = read();
        int minute = read();
        int second = read();
        Calendar calendar = Calendar.getInstance();
        calendar.set(0, 0, 0, hour, minute, second);
        return new Time(calendar.getTimeInMillis());
    }

    public Date readDate() {
        byte length = read();
        int year = readUB2();
        byte month = read();
        byte date = read();
        int hour = read();
        int minute = read();
        int second = read();
        if (length == 11) {
            long nanos = readUB4();
            Calendar cal = getLocalCalendar();
            cal.set(year, --month, date, hour, minute, second);
            Timestamp time = new Timestamp(cal.getTimeInMillis());
            time.setNanos((int) nanos);
            return time;
        } else {
            Calendar cal = getLocalCalendar();
            cal.set(year, --month, date, hour, minute, second);
            return new java.sql.Date(cal.getTimeInMillis());
        }
    }

    public BigDecimal readBigDecimal() throws IOException {
        String src = readStringWithLength();
        return src == null ? null : new BigDecimal(src);
    }
}

