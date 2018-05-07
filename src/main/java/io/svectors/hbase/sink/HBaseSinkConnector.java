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

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import com.google.common.collect.Lists;

/**
 * @author ravi.magham
 */
public class HBaseSinkConnector extends SinkConnector {

    public static final String VERSION = "1.0";
    private Map<String, String> configProperties;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HBaseSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = Lists.newArrayList();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(configProperties);
        }
        return configs;
    }

    @Override
    public void stop() {
        // NO-OP
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }
}
