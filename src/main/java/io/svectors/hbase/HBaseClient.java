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
package io.svectors.hbase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

import io.svectors.hbase.sink.SinkConnectorException;
import io.svectors.hbase.util.TrackHbaseWrite;

/**
 * @author ravi.magham
 */
public final class HBaseClient implements TrackHbaseWrite{

    private static final String HBASE_PRODUCER_TOPIC = "hbase.producer.topic";
    private long lastWrittenAt = System.currentTimeMillis();
    private String lastWrittenUuid="";
    private Properties producerProp = new Properties();
    private Producer<String, JsonNode> hbaseProducer = null;
    private final static Logger logger = LoggerFactory.getLogger(HBaseClient.class);
    private final HBaseConnectionFactory connectionFactory;
    private String producerTopic;
    private Connection connection;
    private boolean producerEnabled = false;
    private static final ObjectMapper mapper = new ObjectMapper();

    public HBaseClient(final HBaseConnectionFactory connectionFactory) throws Exception {
        this.connectionFactory = connectionFactory;
        connection = establishConnection();
        if (producerEnabled) {
            producerProp.put("bootstrap.servers", "localhost:9092");
            producerProp.put("key.serializer",
                    "org.apache.kafka.connect.json.JsonSerializer");
            producerProp.put("value.serializer",
                    "org.apache.kafka.connect.json.JsonSerializer");
            producerProp.put("linger.ms", 1000);
            this.hbaseProducer = new KafkaProducer<String, JsonNode>(producerProp);
        }
    }

    public void write(final String tableName, final List<Put> puts) throws Exception {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(puts);
        final TableName table = TableName.valueOf(tableName);
        write(table, puts);
    }

    public void write(final TableName table, final List<Put> puts) throws Exception {
        Preconditions.checkNotNull(table);
        Preconditions.checkNotNull(puts);
        try {
            final BufferedMutator mutator = connection
                    .getBufferedMutator(table);
            mutator.mutate(puts);
            mutator.flush();
            for (Put put : puts) {
                byte[] id = put.getRow();
                String stringUuid = new String(id);
                if (producerEnabled) {
                    Get getId = new Get(id);
                    Table getTable = connection.getTable(table);
                    if (getTable.exists(getId)) {
                        JsonNode record = new ObjectMapper().createObjectNode();
                        NavigableMap<byte[], List<Cell>> cellMap = put
                                .getFamilyCellMap();
                        List<Cell> cellList = cellMap.firstEntry().getValue();
                        for (Cell cell : cellList) {
                            String qualifier = new String(
                                    CellUtil.cloneQualifier(cell));
                            byte[] valueByte = CellUtil.cloneValue(cell);
                            String valueString = new String(valueByte);
                            boolean isValidJson = isJSONValid(valueString);
                            if (!isValidJson) {
                                ((ObjectNode) record).put(qualifier, valueString);
                            } else {
                                ((ObjectNode) record).set(qualifier,
                                        mapper.readTree(
                                                new ByteArrayInputStream(
                                                        valueByte)));
                            }
                        }
                        logger.info("pushing a new record with id: "
                                + stringUuid + " to " + producerTopic);
                        hbaseProducer.send(new ProducerRecord<String, JsonNode>(
                                producerTopic, record));
                    } else {
                        logger.info("Something went wrong. " + stringUuid
                                + " does not exist in Hbase");
                    }
                } else {
                    logger.info("Your producer configuration is disabled");
                }
                this.lastWrittenUuid = stringUuid;
                this.lastWrittenAt = System.currentTimeMillis();
            }

        } catch (Exception ex) {
            logger.error(String.format(
                    "Failed with a [%s] when writing to table [%s] ",
                    ex.getMessage(), table.getNameAsString()));
            final String errorMsg = String.format(
                    "Failed with a [%s] when writing to table [%s] ",
                    ex.getMessage(), table.getNameAsString());
            throw new SinkConnectorException(errorMsg, ex);
        }
    }

    public Connection establishConnection() throws Exception {
        final Connection connection = this.connectionFactory.getConnection();
        if (connection.getConfiguration().getRaw(HBASE_PRODUCER_TOPIC) != null) {
            this.producerTopic = connection.getConfiguration().get(HBASE_PRODUCER_TOPIC);
            this.producerEnabled = true;
        }
        return connection;
    }

    @Override
    public long getRecentTransactionTime() {
        return this.lastWrittenAt;
    }

    @Override
    public String getLastWrittenUUid() {
        return lastWrittenUuid;
    }

    private static boolean isJSONValid(String jsonInString ) {
        try {
           mapper.readTree(jsonInString);
           return true;
        } catch (IOException e) {
           return false;
        }
      }
}


