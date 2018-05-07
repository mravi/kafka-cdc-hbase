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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;

/**
 * Parses a json event.
 * @author ravi.magham
 * @author dev.anand 20170731
 */
public class JsonEventParser implements EventParser {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final static ObjectReader JSON_READER = OBJECT_MAPPER.reader(JsonNode.class);
    
    private final static Gson gson = new Gson();

    private final JsonConverter keyConverter;
    private final JsonConverter valueConverter;

    /**
     * default c.tor
     */
    public JsonEventParser() {
        this.keyConverter = new JsonConverter();
        this.valueConverter = new JsonConverter();

        Map<String, String> props = new HashMap<>(1);
        props.put("schemas.enable", Boolean.FALSE.toString());

        this.keyConverter.configure(props, true);
        this.valueConverter.configure(props, false);

    }

    @Override
    public Map<String, byte[]> parseKey(SinkRecord sr) throws EventParsingException {
        return this.parse(sr.topic(), sr.key(), true);
    }

    @Override
    public Map<String, byte[]> parseValue(SinkRecord sr) throws EventParsingException {
        return this.parse(sr.topic(), sr.value(), false);
    }

    /**
     * Parses the value.
     * @param topic
     * @param schema
     * @param value
     * @return
     * @throws EventParsingException
     */
    public Map<String, byte[]> parse(final String topic, final Object value, final boolean isKey)
        throws EventParsingException {
        final Map<String, byte[]> values = new LinkedHashMap<>();
        try {
            byte[] valueBytes = null;
            if(isKey) {
                valueBytes = keyConverter.fromConnectData(topic, null, value);
            } else {
                valueBytes = valueConverter.fromConnectData(topic, null, value);
            }
            if(valueBytes == null || valueBytes.length == 0) {
                return Collections.emptyMap();
            }

            final JsonNode valueNode = JSON_READER.readValue(valueBytes);
            final Map<String, Object> keyValues = OBJECT_MAPPER.convertValue(valueNode,
              new TypeReference<Map<String, Object>>() {});

            final List<String> keys = new ArrayList<String>();
            keys.addAll(keyValues.keySet());
            for(String key : keys) {
                final byte[] fieldValue = toValue(keyValues, key);
                if(fieldValue == null) {
                    continue;
                }
                values.put(key, fieldValue);
            }
            return values;
        } catch (Exception ex) {
            final String errorMsg = String.format("Failed to parse [%s] with ex [%s]" ,
                    value, ex.getMessage());
            
            throw new EventParsingException(errorMsg, ex);
        }
    }

    /**
     *
     * @param keyValues
     * @param field
     * @return
     */
    private byte[] toValue(final Map<String, Object> keyValues, final String field) {
        Preconditions.checkNotNull(field);     
       
       final Object fieldValue = keyValues.get(field);
       if (fieldValue instanceof String) {
           return ((String)fieldValue).getBytes();
       } else if (fieldValue instanceof Integer) {
           return Bytes.toBytes((Integer)fieldValue);
       } else if (fieldValue instanceof Double) {
           return Bytes.toBytes((Double)fieldValue);
       } else if (fieldValue instanceof Long) {
           return Bytes.toBytes((Long)fieldValue);
       } else if (fieldValue instanceof Boolean) {
           return Bytes.toBytes((Boolean)fieldValue);
       } else if (fieldValue instanceof Map) {
           String jsonString = gson.toJson(fieldValue);
           return jsonString.getBytes();
       } else if (fieldValue instanceof List) {
           String jsonString = gson.toJson(fieldValue);
           return jsonString.getBytes();
       } else {
           final String errorMsg = String.format("Unable to parse [%s] of type [%s]" ,
                   field, fieldValue.getClass());
           throw new EventParsingException(errorMsg);
       }
    }
}
