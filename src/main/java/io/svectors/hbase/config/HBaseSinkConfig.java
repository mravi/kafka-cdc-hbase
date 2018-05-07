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
package io.svectors.hbase.config;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkTask;
import org.eclipse.jetty.util.StringUtil;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import io.svectors.hbase.parser.EventParser;

/**
 * @author ravi.magham
 */
public class HBaseSinkConfig extends AbstractConfig {
    final static Logger logger = Logger.getLogger(HBaseSinkConfig.class);
    public static final String ZOOKEEPER_QUORUM_CONFIG = "zookeeper.quorum";
    public static final String EVENT_PARSER_CONFIG = "event.parser.class";
    public static String DEFAULT_HBASE_ROWKEY_DELIMITER = ",";
    public static String DEFAULT_HBASE_COLUMN_FAMILY = "d";

    /*
     * The configuration for a table "test" will be in the format
     * hbase.test.rowkey.columns = id , ts
     * hbase.test.rowkey.delimiter = |
     */
    public static final String TABLE_ROWKEY_COLUMNS_TEMPLATE = "hbase.%s.rowkey.columns";
    public static final String TABLE_ROWKEY_DELIMITER_TEMPLATE = "hbase.%s.rowkey.delimiter";
    public static final String TABLE_COLUMN_FAMILY_TEMPLATE = "hbase.%s.family";

    private static ConfigDef CONFIG = new ConfigDef();
    private Map<String, String> properties;

    static {

        CONFIG.define(ZOOKEEPER_QUORUM_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Zookeeper quorum " +
                "of the hbase cluster");

        CONFIG.define(EVENT_PARSER_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Event parser class " +
                "to parse the SinkRecord");

    }

    public HBaseSinkConfig(Map<String, String> originals) {
        this(CONFIG, originals);
    }

    public HBaseSinkConfig(ConfigDef definition, Map<String, String> originals) {
        super(definition, originals);
        this.properties = originals;
    }

    /**
     * Validates the properties to ensure the rowkey property is configured for each table.
     */
    public void  validate() {
        //enrich this
        final String topicsAsStr = properties.get(SinkTask.TOPICS_CONFIG);
        final String[] topics = topicsAsStr.split(",");
        for (String topic : topics) {
            String key = String.format(TABLE_ROWKEY_COLUMNS_TEMPLATE, topic);
            if(!properties.containsKey(key)) {
                throw new ConfigException(String.format(" No rowkey has been configured for table [%s]", key));
            }
        }
        final String zookeeperQuorum = properties.get(ZOOKEEPER_QUORUM_CONFIG);
        if (StringUtil.isBlank(zookeeperQuorum)) {
            logger.info("ZOOKEEPER_QUORUM_CONFIG cannot be null or empty");
            throw new ConfigException(String.format(zookeeperQuorum+" Empty or Null"));
        }
        final String name = properties.get("name");
        if (StringUtil.isBlank(name)) {
            logger.error("Name cannot be null or empty");
            throw new ConfigException(String.format(name+" Empty or Null"));
        }
    }

    /**
     * Instantiates and return the event parser .
     * @return
     */
    public EventParser eventParser()  {
        try {
            final String eventParserClass = getString(EVENT_PARSER_CONFIG);
            final Class<? extends EventParser> eventParserImpl = (Class<? extends EventParser>) Class.forName(eventParserClass);
            return eventParserImpl.newInstance();
        } catch (ClassNotFoundException | InstantiationException  | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param propertyName
     * @param defaultValue
     * @return
     */
    public String getPropertyValue(final String propertyName, final String defaultValue) {
        String propertyValue = getPropertyValue(propertyName);
        return propertyValue != null ? propertyValue : defaultValue;
    }

    /**
     * @param propertyName
     * @return
     */
    public String getPropertyValue(final String propertyName) {
        Preconditions.checkNotNull(propertyName);
        return this.properties.get(propertyName);
    }
}
