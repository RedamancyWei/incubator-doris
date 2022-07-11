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

import org.apache.doris.common.Config;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.privilege.PaloAuth;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Connection implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(Connection.class);

    private static final int BUFFER_SIZE = 1024;
    private static final int ERROR_STATUS = 0xff;
    private static final int OK_STATUS = 0x00;

    private final StringBuilder sb = new StringBuilder();

    private String host;
    private int port;
    private String username;
    private String password;
    private String database;

    private Socket socket;

    private InputStream in;
    private OutputStream out;

    private ByteBuffer hpBuffer;
    private ByteBuffer buffer;

    private HandshakePacket handshakePacket;

    private Boolean initialized = false;

    public Connection() {
    }

    /**
     * Fe query be does not require a password,
     * root and admin user is allowed to login from 127.0.0.1, in case user forget password.
     *
     * @param database database name
     * @see PaloAuth
     */
    public Connection(String database) {
        host = "127.0.0.1";
        port = Config.query_port;
        username = PaloAuth.ROOT_USER;
        password = "";
        this.database = database;
    }

    public Connection(String host, int port, String username, String password, String database) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public Boolean getInitialized() {
        return initialized;
    }

    public void setInitialized(Boolean initialized) {
        this.initialized = initialized;
    }

    public static int getClientCapabilities() {
        int flag = 0;
        flag |= MysqlCapability.Flag.CLIENT_LONG_PASSWORD.getFlagBit();
        flag |= MysqlCapability.Flag.CLIENT_FOUND_ROWS.getFlagBit();
        flag |= MysqlCapability.Flag.CLIENT_LONG_FLAG.getFlagBit();
        flag |= MysqlCapability.Flag.CLIENT_CONNECT_WITH_DB.getFlagBit();
        flag |= MysqlCapability.Flag.CLIENT_ODBC.getFlagBit();
        flag |= MysqlCapability.Flag.CLIENT_IGNORE_SPACE.getFlagBit();
        flag |= MysqlCapability.Flag.CLIENT_PROTOCOL_41.getFlagBit();
        flag |= MysqlCapability.Flag.CLIENT_INTERACTIVE.getFlagBit();
        flag |= MysqlCapability.Flag.CLIENT_IGNORE_SIGPIPE.getFlagBit();
        flag |= MysqlCapability.Flag.CLIENT_TRANSACTIONS.getFlagBit();
        flag |= MysqlCapability.Flag.CLIENT_SECURE_CONNECTION.getFlagBit();
        return flag;
    }

    /**
     * Connection is done by many exchanges:
     * (Create socket)
     * 1. Server sends Initial handshake packet
     * - If SSL/TLS connection
     * - Client sends SSLRequest packet and switches to SSL mode for sending and receiving the following messages:
     * 2. Client sends Handshake response packet
     * 3. Server sends either:
     * - An OK packet in case of success OkPacket
     * - An error packet in case of error ErrPacket
     * - Authentication switch
     * - If the client or server doesn't have PLUGIN_AUTH capability:
     * - Server sends 0xFE byte
     * - Client sends old_password
     * - else
     * - Server sends Authentication switch request
     * - Client may have many exchange with the server according to the Plugin.
     * 4. Authentication switch ends with server sending either OkPacket or ErrPacket
     */
    public void init() throws IOException, NoSuchAlgorithmException {
        if (!initialized) {
            InetSocketAddress address = new InetSocketAddress(host, port);
            socket = new Socket();
            socket.connect(address);
            in = socket.getInputStream();
            out = socket.getOutputStream();

            hpBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            buffer = ByteBuffer.allocate(BUFFER_SIZE);

            // get handshake packet from server
            getHandshakePacket();

            // send handshake packet
            sendAuthPacket();

            // handle response packet
            handleResponsePacket();

            // Already initialized
            initialized = true;
        }
    }

    /**
     * close socket and input/output stream
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (socket != null) {
            if (socket.isInputShutdown()) {
                socket.shutdownInput();
            }
            if (socket.isOutputShutdown()) {
                socket.shutdownOutput();
            }
            if (socket.isClosed()) {
                socket.close();
            }
        }

        if (in != null) {
            in.close();
            in = null;
        }

        if (out != null) {
            out.close();
            out = null;
        }

        initialized = false;
    }

    /**
     * Used to execute statements that do not return.
     */
    public void execute(String sql) throws IOException, NoSuchAlgorithmException {
        // init client
        init();

        // send sql to server
        sendStatement(sql);

        // get data from packet
        parseResultPacket();

        LOG.info("FE queries BE information: {}", sb);
    }

    /**
     * Used to execute statements with returns.
     */
    public QueryResultSet query(String sql) throws IOException, NoSuchAlgorithmException {
        // init client
        init();

        // send sql to server
        sendStatement(sql);

        // get data from packet
        ResultSetPacket packet = parseResultPacket();

        LOG.info("FE queries BE information: {}", sb);
        return new QueryResultSet(packet);
    }

    /**
     * Read the handshake packet data sent by the server to the client.
     *
     * @throws IOException if an I/O error occurs
     */
    private void getHandshakePacket() throws IOException {
        checkIfClientIsOpened();

        byte[] data = new byte[BUFFER_SIZE];
        int read = in.read(data);

        if (read == 0) {
            throw new IOException("socket closed");
        }

        hpBuffer.put(data);
        hpBuffer.flip();

        // get the packet sent by the server
        handshakePacket = new HandshakePacket();
        handshakePacket.read(hpBuffer);

        // append the handshake packet to the log
        sb.append("HandshakePacket:\n").append(handshakePacket).append("\n");
    }

    /**
     * Send an authentication packet to the server
     *
     * @throws IOException if an I/O error occurs or failed to encrypted login password
     */
    private void sendAuthPacket() throws IOException, NoSuchAlgorithmException {
        AuthPacket packet = new AuthPacket();

        // set the packet content
        packet.packetSequenceId = 1;
        packet.capabilityFlags = getClientCapabilities();
        packet.characterSet = handshakePacket.characterSet;
        packet.username = username;

        // Encrypted login password
        int len1 = handshakePacket.authPluginDataPart1.length;
        int len2 = handshakePacket.authPluginDataPart2.length;
        byte[] seed = new byte[len1 + len2];
        System.arraycopy(handshakePacket.authPluginDataPart1, 0, seed, 0, len1);
        System.arraycopy(handshakePacket.authPluginDataPart2, 0, seed, len1, len2);
        packet.password = scramble411(password.getBytes(), seed);

        packet.database = database;
        packet.authPluginName = handshakePacket.authPluginName;
        ByteBuffer apBuffer = ByteBuffer.allocate(packet.calcPacketSize() + 4);
        packet.write(apBuffer);

        // send the authentication packet
        out.write(apBuffer.array(), 0, apBuffer.limit());
        out.flush();

        // append the authentication packet to the log
        sb.append("\nAuthPacket:\n").append(packet).append("\n");
    }

    /**
     * Receive server response information, which may be OK Packet or ERR Packet
     *
     * @throws IOException if an I/O error occurs
     */
    private void handleResponsePacket() throws IOException {
        buffer.clear();
        int length = in.read(buffer.array());
        buffer.limit(length);
        int status = buffer.get(4) & ERROR_STATUS;

        if (status == ERROR_STATUS) {
            handleErrPacket();
        }

        if (status == OK_STATUS) {
            handleOkPacket();
        }
    }

    /**
     * parse packet-ProtocolText result set
     *
     * @throws IOException if an I/O error occurs
     */
    private ResultSetPacket parseResultPacket() throws IOException {
        buffer.clear();

        int length = in.read(buffer.array());
        buffer.limit(length);
        int status = buffer.get(4) & ERROR_STATUS;

        if (status == ERROR_STATUS) {
            handleErrPacket();
            return null;
        }

        ResultSetPacket resultPacket = new ResultSetPacket();
        resultPacket.read(buffer);

        // append the result set packet to the log
        sb.append("\nResultSetPacket:\n").append(resultPacket).append("\n");

        return resultPacket;
    }

    private void sendStatement(String sql) throws IOException {
        ByteBuffer queryBuffer = ByteBuffer.allocate(4 + 1 + sql.length());
        BufferUtil.writeUB3(queryBuffer, sql.length() + 1);

        queryBuffer.put((byte) 0);
        queryBuffer.put((byte) MysqlCommand.COM_QUERY.ordinal());
        queryBuffer.put(sql.getBytes());
        queryBuffer.flip();

        out.write(queryBuffer.array());

        // append the sql statement to the log
        sb.append("\nsql statement:\n").append(sql).append("\n");
    }

    private void checkIfClientIsOpened() throws IOException {
        if (socket == null || socket.isClosed()) {
            throw new IOException("Socket is not opened or closed, call init() first.");
        }

        if (in == null || out == null || hpBuffer == null) {
            throw new IOException("Unable to read/write data to/from socket, or unable to allocate buffer.");
        }
    }

    private void handleErrPacket() {
        ErrPacket ep = new ErrPacket();
        ep.capabilities = handshakePacket.capabilities;
        ep.read(buffer);
        sb.append("\nErrPacket:\n").append(ep).append("\n");
    }

    private void handleOkPacket() {
        OkPacket op = new OkPacket();
        op.read(buffer);
        sb.append("\nOkPacket:\n").append(op).append("\n");
    }

    private static byte[] scramble411(byte[] pass, byte[] seed) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] pass1 = md.digest(pass);
        md.reset();
        byte[] pass2 = md.digest(pass1);
        md.reset();
        md.update(seed);
        byte[] pass3 = md.digest(pass2);

        for (int i = 0; i < pass3.length; ++i) {
            pass3[i] ^= pass1[i];
        }

        return pass3;
    }
}
