/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.svectors.hbase.sink;

import static io.svectors.hbase.sink.HbaseTestUtil.createTable;
import static io.svectors.hbase.sink.HbaseTestUtil.getUtility;
import static io.svectors.hbase.sink.HbaseTestUtil.startMiniCluster;
import static io.svectors.hbase.sink.HbaseTestUtil.stopMiniCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.svectors.hbase.config.HBaseSinkConfig;
import io.svectors.hbase.parser.AvroEventParser;
import io.svectors.hbase.parser.JsonEventParser;


/**
 * Integration Test of HBase sink.
 *
 * @author ravi.magham
 * @author dev.anand 20170731
 */

public class TestHbaseSinkTask {

    private final Function<Integer, String> TO_LOCAL_URI = (port) -> "localhost:" + port;
    private final String hbaseTable = "test"; // using this interchangeably with kafka topic name.
    private final String columnFamily = "d";
    private final Map<String, String> configProps = new HashMap<>();
    private Configuration configuration;

    @Before
    public void setUp() throws Exception {
        startMiniCluster();
        createTable(hbaseTable, columnFamily);
        configuration = getUtility().getConfiguration();

        //configure defaults for Sink task.
        configProps.put("hbase.test.rowkey.columns", "id");
        configProps.put("hbase.test.rowkey.delimiter", "|");
        configProps.put("hbase.test.family", columnFamily);
        configProps.put(SinkTask.TOPICS_CONFIG, hbaseTable);
        configProps.put("name", "name");
        configProps.put(HBaseSinkConfig.ZOOKEEPER_QUORUM_CONFIG, TO_LOCAL_URI.apply(getUtility().getZkCluster()
                .getClientPort()));
    }

    @Test
    public void testConnectUsingJsonEventParser() throws Exception {
        configProps.put(HBaseSinkConfig.EVENT_PARSER_CONFIG, JsonEventParser.class.getName());
        writeAndValidateJson();
    }

    @Test
    public void testConnectUsingAvroEventParser() throws Exception {
        configProps.put(HBaseSinkConfig.EVENT_PARSER_CONFIG, AvroEventParser.class.getName());
        writeAndValidate();
    }

    /**
     * Performs write through kafka connect and validates the data in hbase.
     *
     * @throws IOException
     */
    private void writeAndValidate() throws IOException {
        HBaseSinkTask task = new HBaseSinkTask();
        task.start(configProps);

        final Schema valueSchema = SchemaBuilder.struct().name("record").version(1)
                .field("url", Schema.STRING_SCHEMA)
                .field("id", Schema.INT32_SCHEMA)
                .field("zipcode", Schema.INT32_SCHEMA)
                .field("status", Schema.INT32_SCHEMA)
                .build();

        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        int noOfRecords = 10;
        for (int i = 1; i <= noOfRecords; i++) {
            final Struct record = new Struct(valueSchema)
                    .put("url", "google.com")
                    .put("id", i)
                    .put("zipcode", 95050 + i)
                    .put("status", 400 + i);
            SinkRecord sinkRecord = new SinkRecord(hbaseTable, 0, null, null, valueSchema, record, i);
            sinkRecords.add(sinkRecord);
        }

        task.put(sinkRecords);

        // read from hbase.
        TableName table = TableName.valueOf(hbaseTable);
        Scan scan = new Scan();
        try (Table hTable = ConnectionFactory.createConnection(configuration).getTable(table);
                ResultScanner results = hTable.getScanner(scan);) {
            int count = 0;
            for (Result result : results) {
                int rowId = Bytes.toInt(result.getRow());
                String url = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("url")));
                Assert.assertEquals(count + 1, rowId);
                Assert.assertEquals("google.com", url);
                count++;
            }
            Assert.assertEquals(noOfRecords, count);
        }
        task.stop();
    }

    /**
     * Performs write through kafka connect and validates the data in hbase.
     *
     * @throws IOException
     */
    private void writeAndValidateJson() throws IOException {
        HBaseSinkTask task = new HBaseSinkTask();
        task.start(configProps);

        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        int noOfRecords = 10;
        for (int i = 1; i <= noOfRecords; i++) {
            final HashMap<String, Object> record = new HashMap<String, Object>();
            record.put("url", "google.com");
            record.put("id", i);
            record.put("zipcode", 95050 + i);
            record.put("status", 400 + 1);
            final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, null, record, 0);

            sinkRecords.add(sinkRecord);
        }

        task.put(sinkRecords);

        // read from hbase.
        TableName table = TableName.valueOf(hbaseTable);
        Scan scan = new Scan();
        try (Table hTable = ConnectionFactory.createConnection(configuration).getTable(table);
                ResultScanner results = hTable.getScanner(scan);) {
            int count = 0;
            for (Result result : results) {
                int rowId = Bytes.toInt(result.getRow());
                String url = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("url")));
                Assert.assertEquals(count + 1, rowId);
                Assert.assertEquals("google.com", url);
                count++;
            }
            Assert.assertEquals(noOfRecords, count);
        }
        task.stop();
    }

    @After
    public void tearDown() throws Exception {
        stopMiniCluster();
    }
}
