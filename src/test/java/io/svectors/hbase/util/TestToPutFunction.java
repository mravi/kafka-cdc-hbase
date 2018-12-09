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
package io.svectors.hbase.util;

import io.svectors.hbase.config.HBaseSinkConfig;
import io.svectors.hbase.parser.JsonEventParser;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ravi.magham
 */
public class TestToPutFunction {

    @Test
    public void testRowkey() {
        final Map<String, String> configProps = new HashMap<>();
        configProps.put(HBaseSinkConfig.ZOOKEEPER_QUORUM_CONFIG, "localhost");
        configProps.put("hbase.test.rowkey.columns", "id");
        configProps.put("hbase.test.d.columns", "d1,d2");
        configProps.put(HBaseSinkConfig.EVENT_PARSER_CONFIG, JsonEventParser.class.getName());
        final ToPutFunction toPutFunction = new ToPutFunction(new HBaseSinkConfig(configProps));

        final Schema schema = SchemaBuilder.struct().name("record").version(1)
          .field("url", Schema.STRING_SCHEMA)
          .field("id", Schema.INT32_SCHEMA)
          .field("zipcode", Schema.INT32_SCHEMA)
          .field("status", Schema.BOOLEAN_SCHEMA)
          .build();

        final Struct record = new Struct(schema)
          .put("url", "google.com")
          .put("id", 123456)
          .put("zipcode", 95051)
          .put("status", true);

        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, schema, record, 0);
        final Put put =  toPutFunction.apply(sinkRecord);
        Assert.assertEquals(123456, Bytes.toInt(put.getRow()));
    }
}
