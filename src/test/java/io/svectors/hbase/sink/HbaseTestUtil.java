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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class for hbase tests.
 *
 * @author ravi.magham
 */
public abstract class HbaseTestUtil {
    final static Logger logger = Logger.getLogger(HbaseTestUtil.class);
    /* status of the cluster */
    private static AtomicBoolean status = new AtomicBoolean();
    private static AtomicReference<HBaseTestingUtility> utility = new AtomicReference<>();

    /**
     * Returns a new HBaseTestingUtility instance.
     */
    private static HBaseTestingUtility createTestingUtility() {
        final Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("replication.stats.thread.period.seconds", 5);
        hbaseConf.setLong("replication.sleep.before.failover", 2000);
        hbaseConf.setInt("replication.source.maxretriesmultiplier", 10);
        return new HBaseTestingUtility(hbaseConf);
    }

    public static HBaseTestingUtility getUtility() {
        HBaseTestingUtility testingUtility = utility.get();
        if (testingUtility == null) {
            testingUtility = createTestingUtility();
            utility.set(testingUtility);
        }
        return testingUtility;
    }

    /**
     * start the mini cluster
     */
    public static void startMiniCluster() {
        if (status.compareAndSet(false, true)) {
            try {
                getUtility().startMiniCluster();
            } catch (Exception e) {
                status.set(false);
                throw new RuntimeException("Unable to start the hbase mini cluster", e);
            }
        }
    }

    /**
     * stops the mini cluster
     */
    public static void stopMiniCluster() {
        HBaseTestingUtility testingUtility = getUtility();
        if (testingUtility != null && status.compareAndSet(true, false)) {
            try {
                testingUtility.shutdownMiniCluster();
            } catch (Exception e) {
                status.set(true);
                throw new RuntimeException("Unable to shutdown MiniCluster", e);
            }
        }
    }

    /**
     * Creates the table with the column families
     *
     * @param tableName
     * @param columnFamilies
     * @return
     */
    public static void createTable(String tableName, String... columnFamilies) {
      
        HBaseTestingUtility testingUtility = getUtility();
        if (!status.get()) {
            throw new RuntimeException("The mini cluster hasn't started yet. " +
              " Call HBaseTestUtil#startMiniCluster() before creating a table");
        }
        final TableName name = TableName.valueOf(tableName);
        try (HBaseAdmin hBaseAdmin = testingUtility.getHBaseAdmin()) {
            final HTableDescriptor hTableDescriptor = new HTableDescriptor(name);
            for (String family : columnFamilies) {
                final HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(family));
                hTableDescriptor.addFamily(hColumnDescriptor);
            }

            hBaseAdmin.createTable(hTableDescriptor);
            testingUtility.waitUntilAllRegionsAssigned(name);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
