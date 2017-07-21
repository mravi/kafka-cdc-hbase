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
package io.svectors.hbase.parser;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author ravi.magham
 */
public class TestJsonEventParser {

    private JsonEventParser eventParser;

    @Before
    public void setup() {
        eventParser = new JsonEventParser();
    }

    @Test
    public void testParseValue() {
       
        String url = "google.com";
        int id = 1;
        int zipcode = 95051;
        boolean status = true;

        final HashMap<String, Object> record = new HashMap<String, Object>();
        record.put("url", url);
        record.put("id", id);
        record.put("zipcode", zipcode);
        record.put("status", status);
        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, null, record, 0);

        Map<String, byte[]> result = eventParser.parseValue(sinkRecord);
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(url, Bytes.toString(result.get("url")));
        Assert.assertEquals(id, Bytes.toInt(result.get("id")));
        Assert.assertEquals(zipcode, Bytes.toInt(result.get("zipcode")));
        Assert.assertEquals(status, Bytes.toBoolean(result.get("status")));
    }

    @Test
    public void testParseNullKey() {
        final SinkRecord sinkRecord = new SinkRecord("test", 0, null, null, null, null, 0);
        final Map<String, byte[]> keys = eventParser.parseKey(sinkRecord);
        Assert.assertTrue(keys.isEmpty());
    }
}
