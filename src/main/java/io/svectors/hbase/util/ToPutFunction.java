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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import io.svectors.hbase.config.HBaseSinkConfig;
import io.svectors.hbase.parser.EventParser;

/**
 * @author ravi.magham
 */
public class ToPutFunction implements Function<SinkRecord, Put> {

	private final HBaseSinkConfig sinkConfig;
	private final EventParser eventParser;

	static Logger logger = Logger.getLogger(ToPutFunction.class);
	public ToPutFunction(HBaseSinkConfig sinkConfig) {
		this.sinkConfig = sinkConfig;
		this.eventParser = sinkConfig.eventParser();
	}

	/**
	 * Converts the sinkRecord to a {@link Put} instance The event parser parses
	 * the key schema of sinkRecord only when there is no property configured
	 * for {@link HBaseSinkConfig#TABLE_ROWKEY_COLUMNS_TEMPLATE}
	 *
	 * @param sinkRecord
	 * @return
	 */
	@Override
	public Put apply(final SinkRecord sinkRecord) {
		Preconditions.checkNotNull(sinkRecord);
		final String table = sinkRecord.topic();
		final String columnFamily = columnFamily(table);
		final String delimiter = rowkeyDelimiter(table);
		String[] cfArr = columnFamily.split(",");

		final Map<String, byte[]> valuesMap = this.eventParser.parseValue(sinkRecord);
		final Map<String, byte[]> keysMap = this.eventParser.parseKey(sinkRecord);

		valuesMap.putAll(keysMap);
		final String[] rowkeyColumns = rowkeyColumns(table);
		final byte[] rowkey = toRowKey(valuesMap, rowkeyColumns, delimiter);

		final Put put = new Put(rowkey);
		/**
		 * If there are more than one column families, columns mapping is read from configuration
		 */
		if(cfArr.length > 1){
			for(String cf: cfArr){
				List<String> columnList = columnMapping(cf,table);
				for(String column : columnList){
					List<byte[]> valueList = valuesMap.entrySet().stream()
							.filter(map -> column.equals(map.getKey()))
							.map(Map.Entry::getValue).collect(Collectors.toList());
					byte[] colValue = valueList.get(0);
				 	put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), colValue);
				}
			}
		}else{
			valuesMap.entrySet().stream().forEach(entry -> {
				final String qualifier = entry.getKey();
				final byte[] value = entry.getValue();
				put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), value);
			});
		}
		return put;
	}

	/**
	 * A kafka topic is a 1:1 mapping to a HBase table.
	 * 
	 * @param table
	 * @return
	 */
	private String[] rowkeyColumns(final String table) {
		final String entry = String.format(HBaseSinkConfig.TABLE_ROWKEY_COLUMNS_TEMPLATE, table);
		final String entryValue = sinkConfig.getPropertyValue(entry);
		return entryValue.split(",");
	}

	/**
	 * Returns the delimiter for a table. If nothing is configured in
	 * properties, we use the default
	 * {@link HBaseSinkConfig#DEFAULT_HBASE_ROWKEY_DELIMITER}
	 * 
	 * @param table
	 *            hbase table.
	 * @return
	 */
	private String rowkeyDelimiter(final String table) {
		final String entry = String.format(HBaseSinkConfig.TABLE_ROWKEY_DELIMITER_TEMPLATE, table);
		final String entryValue = sinkConfig.getPropertyValue(entry, HBaseSinkConfig.DEFAULT_HBASE_ROWKEY_DELIMITER);
		return entryValue;
	}

	/**
	 * Returns the column family mapped in configuration for the table. If not
	 * present, we use the default
	 * {@link HBaseSinkConfig#DEFAULT_HBASE_COLUMN_FAMILY}
	 * 
	 * @param table
	 *            hbase table.
	 * @return
	 */
	private String columnFamily(final String table) {
		final String entry = String.format(HBaseSinkConfig.TABLE_COLUMN_FAMILY_TEMPLATE, table);
		final String entryValue = sinkConfig.getPropertyValue(entry.trim(), HBaseSinkConfig.DEFAULT_HBASE_COLUMN_FAMILY);
		return entryValue;
	}

	/**
	 * Returns the columns mapping for column family
	 * {@link HBaseSinkConfig#DEFAULT_HBASE_COLUMN_FAMILY}
	 * 
	 * @param table
	 *            hbase table.
	 * @return
	 */
	private List<String> columnMapping(final String columnFamily, final String table) {
		System.out.println(columnFamily);
		System.out.println(table);
		String columnsNameTemplate = String.format(HBaseSinkConfig.TABLE_COLUMN_FAMILY_COLUMNS_TEMPLATE, table,
				columnFamily);
		String columns = sinkConfig.getPropertyValue(columnsNameTemplate);
		logger.info(columns);
		List<String> columnList = Stream.of(columns.split(",")).collect(Collectors.toList());
		logger.info(columnList);
		return columnList;
	}

	/**
	 *
	 * @param valuesMap
	 * @param columns
	 * @return
	 */
	private byte[] toRowKey(final Map<String, byte[]> valuesMap, final String[] columns, final String delimiter) {
		Preconditions.checkNotNull(valuesMap);
		Preconditions.checkNotNull(delimiter);

		byte[] rowkey = null;
		byte[] delimiterBytes = Bytes.toBytes(delimiter);
		for (String column : columns) {
			byte[] columnValue = valuesMap.get(column);
			if (rowkey == null) {
				rowkey = columnValue;
			} else {
				rowkey = Bytes.add(rowkey, delimiterBytes, columnValue);
			}
		}
		return rowkey;
	}
}
