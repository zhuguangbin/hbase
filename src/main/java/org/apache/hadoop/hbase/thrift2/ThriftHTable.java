/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.thrift2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOExceptionWithCause;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.thrift2.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hbase.thrift2.security.token.DelegationTokenSelector;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * HTable interface to remote tables accessed via Thrift gateway
 */
public class ThriftHTable implements HTableInterface {

    private static final Log LOG = LogFactory.getLog(ThriftHTable.class);

    private final THBaseService.Iface thriftClient;
    private TTransport transport;
    final Configuration conf;
    final String[] thriftServers;
    final byte[] tableName;
    final int maxRetries;
    final long sleepTime;
    final int timeout;

    /**
     * Constructor
     * 
     * @param conf
     * @param tableName
     * @throws TTransportException
     * @throws IOException 
     */
    public ThriftHTable(Configuration conf,
            String tableName) throws TTransportException, IOException {
        this(conf, Bytes.toBytes(tableName), null);
    }

    /**
     * Constructor
     * 
     * @param conf
     * @param tableName
     * @param accessToken
     * @throws TTransportException
     * @throws IOException 
     * @deprecated accessToken is not used and will be removed
     */
    @Deprecated
    public ThriftHTable(Configuration conf,
            String tableName, String accessToken) throws TTransportException, IOException {
        this(conf, Bytes.toBytes(tableName), accessToken);
    }

    /**
     * Constructor
     * 
     * @param conf
     * @param tableName
     * @throws TTransportException
     * @throws IOException 
     */
    public ThriftHTable(Configuration conf, byte[] tableName) throws TTransportException, IOException {
        this(conf, tableName, null);
    }

    /**
     * Constructor
     * 
     * @param conf
     * @param tableName
     * @param accessToken
     * @throws TTransportException
     * @throws IOException 
     * @deprecated accessToken is not used and will be removed
     */
    @Deprecated
    public ThriftHTable(Configuration conf, byte[] tableName, String accessToken) throws TTransportException, IOException {
        this.conf = conf;
        this.tableName = tableName;
        this.thriftServers = conf.getStrings("hbase.regionserver.thrift.servers");
        this.maxRetries = conf.getInt("hbase.regionserver.thrift.client.max.retries", 10);
        this.sleepTime = conf.getLong("hbase.regionserver.thrift.client.sleep", 1000);
        this.timeout = conf.getInt("hbase.regionserver.thrift.client.timeout", 1000);

        List<String> thriftServerList = Arrays.asList(thriftServers);
        Collections.shuffle(thriftServerList);
        String server = thriftServerList.get(0);
        String serverHost = server.split(":")[0];
        int serverPort = Integer.parseInt(server.split(":")[1]);

        String serverType = conf.get("hbase.regionserver.thrift.server.type","threadpool");
        boolean nonblocking = "nonblocking".equals(serverType);
        boolean hsha = "hsha".equals(serverType);
        
        this.transport = new TSocket(serverHost, serverPort, timeout);
        if (conf.getBoolean("hbase.regionserver.thrift.framed", false) || nonblocking || hsha) {
            this.transport = new TFramedTransport(transport);
        }
        
        boolean useSasl = conf.getBoolean("hbase.regionserver.thrift.sasl",
                false);
        if (useSasl) {
            try {
                HadoopThriftAuthBridge.Client authBridge = new HadoopThriftAuthBridge()
                        .createClient();

                String tokenStrForm = getTokenStrForm(accessToken);

                if (tokenStrForm != null) {
                    transport = authBridge.createClientTransport(null,
                            serverHost, "DIGEST", tokenStrForm, transport);
                } else {
                    String principalConfig = conf
                            .get("hbase.regionserver.thrift.kerberos.principal");
                    transport = authBridge.createClientTransport(
                            principalConfig, serverHost, "KERBEROS", null,
                            transport);
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        
        TProtocol protocol = null;
        if (conf.getBoolean("hbase.regionserver.thrift.compact", false)) {
            protocol = new TCompactProtocol(transport);
        } else {
            protocol = new TBinaryProtocol(transport);
        }
        this.thriftClient = new THBaseService.Client(protocol);
        this.transport.open();
    }

    private String getTokenStrForm(String tokenSignature) throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        TokenSelector<? extends TokenIdentifier> tokenSelector = new DelegationTokenSelector();

        Token<? extends TokenIdentifier> token = tokenSelector.selectToken(
                tokenSignature == null ? new Text() : new Text(tokenSignature),
                ugi.getTokens());
        return token != null ? token.encodeToUrlString() : null;
    }

    public byte[] getTableName() {
        return tableName.clone();
    }

    public Configuration getConfiguration() {
        return conf;
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift.");
    }

    @Override
    public boolean exists(Get get) throws IOException {

        int retries = 0;
        while (true) {
            try {
                return thriftClient.exists(ByteBuffer.wrap(tableName),
                        ThriftUtilities.getFromHBase(get));
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results)
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift.");
    }

    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException,
            InterruptedException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift.");
    }

    @Override
    public Result get(Get get) throws IOException {

        int retries = 0;
        while (true) {
            try {
                TResult result = this.thriftClient.get(
                        ByteBuffer.wrap(tableName),
                        ThriftUtilities.getFromHBase(get));
                return ThriftUtilities.resultFromThrift(result);
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }

    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {

        int retries = 0;
        while (true) {
            try {
                List<TResult> results = this.thriftClient.getMultiple(
                        ByteBuffer.wrap(tableName),
                        ThriftUtilities.getsFromHBase(gets));
                return ThriftUtilities.resultsFromThrift(results);
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }
    }

    @Override
    public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift , please use getScannerResults. ");
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift , please use getScannerResults. ");
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier)
            throws IOException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift , please use getScannerResults. ");
    }

    public int openScanner(Scan scan) throws IOException {
        int scannerId;

        int retries = 0;
        while (true) {
            try {
                scannerId = this.thriftClient.openScanner(
                        ByteBuffer.wrap(tableName),
                        ThriftUtilities.scanFromHBase(scan));
                return scannerId;
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }

    }

    public int openScanner(byte[] family) throws IOException {

        Scan scan = new Scan();
        scan.addFamily(family);
        return openScanner(scan);
    }

    public int openScanner(byte[] family, byte[] qualifier) throws IOException {

        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return openScanner(scan);
    }
    
    public void closeScanner(int scannerId) throws IOException{
        
        int retries = 0;
        while (true) {
            try {
                this.thriftClient.closeScanner(scannerId);
                return ;
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }
    }

    /**
     * 
     * @param scannerId
     * @param numRows
     * @return
     * @throws IOException
     */

    public Result[] getScannerRows(int scannerId, int numRows) throws IOException {

        int retries = 0;
        while (true) {
            try {
                List<TResult> results = this.thriftClient.getScannerRows(
                        scannerId, numRows);
                return ThriftUtilities.resultsFromThrift(results);
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            } finally {
                try {
                    this.thriftClient.closeScanner(scannerId);
                } catch (Exception e) {
                    IOException ioe = new IOExceptionWithCause(e);
                    throw ioe;
                }
            }
        }

    }
    
    
    public Result[] getScannerResults(Scan scan, int numRows) throws IOException {
        TScan tScan = ThriftUtilities.scanFromHBase(scan);
        int retries = 0;
        while (true) {
            try {
                List<TResult> results = this.thriftClient.getScannerResults(ByteBuffer.wrap(tableName), tScan, numRows);
                return ThriftUtilities.resultsFromThrift(results);
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }
    }
    
    public Result[] getScannerResults(byte[] family, int numRows) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(family);
        return getScannerResults(scan, numRows);
    }
    
    public Result[] getScannerResults(byte[] family, byte[] qualifier, int numRows) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return getScannerResults(scan, numRows);
    }

    @Override
    public void put(Put put) throws IOException {

        int retries = 0;
        while (true) {
            try {
                this.thriftClient.put(ByteBuffer.wrap(tableName),
                        ThriftUtilities.putFromHBase(put));
                return;
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }

    }

    @Override
    public void put(List<Put> puts) throws IOException {

        int retries = 0;
        while (true) {
            try {
                this.thriftClient.putMultiple(ByteBuffer.wrap(tableName),
                        ThriftUtilities.putsFromHBase(puts));
                return;
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }

    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Put put) throws IOException {

        int retries = 0;
        while (true) {
            try {
                return this.thriftClient.checkAndPut(
                        ByteBuffer.wrap(tableName), ByteBuffer.wrap(row),
                        ByteBuffer.wrap(family), ByteBuffer.wrap(qualifier),
                        ByteBuffer.wrap(value),
                        ThriftUtilities.putFromHBase(put));
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }
    }

    @Override
    public void delete(Delete delete) throws IOException {

        int retries = 0;
        while (true) {
            try {
                this.thriftClient.deleteSingle(ByteBuffer.wrap(tableName),
                        ThriftUtilities.deleteFromHBase(delete));
                return;
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }

    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {

        int retries = 0;
        while (true) {
            try {
                this.thriftClient.deleteMultiple(ByteBuffer.wrap(tableName),
                        ThriftUtilities.deletesFromHBase(deletes));
                return;
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }

    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Delete delete) throws IOException {

        int retries = 0;
        while (true) {
            try {
                return this.thriftClient.checkAndDelete(
                        ByteBuffer.wrap(tableName), ByteBuffer.wrap(row),
                        ByteBuffer.wrap(family), ByteBuffer.wrap(qualifier),
                        ByteBuffer.wrap(value),
                        ThriftUtilities.deleteFromHBase(delete));
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        
        int retries = 0;
        while (true) {
            try {
                this.thriftClient.mutateRow(ByteBuffer.wrap(tableName),
                        ThriftUtilities.rowMutationsFromHBase(rm));
                return ;
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }
    }

    @Override
    public Result append(Append append) throws IOException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        
        int retries = 0;
        while (true) {
            try {
                TResult result = this.thriftClient.increment(
                        ByteBuffer.wrap(tableName),
                        ThriftUtilities.incrementFromHBase(increment));
                return ThriftUtilities.resultFromThrift(result);
            } catch (Exception e) {
                retries++;
                IOException ioe = new IOExceptionWithCause(e);

                if (retries >= maxRetries) {
                    throw ioe;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    // IGNORE
                }
            }
        }
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount) throws IOException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount, boolean writeToWAL)
            throws IOException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public boolean isAutoFlush() {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public void flushCommits() throws IOException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public void close() throws IOException {
        this.transport.close();
    }

    @Override
    public RowLock lockRow(byte[] row) throws IOException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public void unlockRow(RowLock rl) throws IOException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public <T extends CoprocessorProtocol> T coprocessorProxy(
            Class<T> protocol, byte[] row) {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(
            Class<T> protocol, byte[] startKey, byte[] endKey,
            Call<T, R> callable) throws IOException, Throwable {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public <T extends CoprocessorProtocol, R> void coprocessorExec(
            Class<T> protocol, byte[] startKey, byte[] endKey,
            Call<T, R> callable, Callback<R> callback) throws IOException,
            Throwable {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public void setAutoFlush(boolean autoFlush) {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public long getWriteBufferSize() {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        throw new UnsupportedOperationException(
                "this method is not supported on thrift. ");
    }

}
