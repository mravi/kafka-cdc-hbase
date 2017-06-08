package io.svectors.hbase.parser;

import com.google.common.base.Preconditions;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ravi.magham
 */
public class AvroEventParser implements EventParser {

    private final static AvroData avroData = new AvroData(100);
    private final Map<String, byte[]> EMPTY_MAP = Collections.emptyMap();

    /**
     * default c.tor
     */
    public AvroEventParser() {
    }

    @Override
    public Map<String, byte[]> parseKey(SinkRecord sr) throws EventParsingException {
        return parse(sr.keySchema(), sr.key());
    }

    @Override
    public Map<String, byte[]> parseValue(SinkRecord sr) throws EventParsingException {
        return parse(sr.valueSchema(), sr.value());
    }

    /**
     * parses the value.
     * @param schema
     * @param value
     * @return
     */
    private Map<String, byte[]> parse(final Schema schema, final Object value) {
        final Map<String, byte[]> values = new LinkedHashMap<>();
        try {
            Object data = avroData.fromConnectData(schema, value);
            if (data == null || !(data instanceof GenericRecord)) {
                return EMPTY_MAP;
            }
            final GenericRecord record = (GenericRecord) data;
            final List<Field> fields = schema.fields();
            for (Field field : fields) {
                final byte[] fieldValue = toValue(record, field);
                if (fieldValue == null) {
                    continue;
                }
                values.put(field.name(), fieldValue);
            }
            return values;
        } catch (Exception ex) {
            final String errorMsg = String.format("Failed to parse the schema [%s] , value [%s] with ex [%s]" ,
               schema, value, ex.getMessage());
            throw new EventParsingException(errorMsg, ex);
        }
    }

    private byte[] toValue(final GenericRecord record, final Field field) {
        Preconditions.checkNotNull(field);
        final Schema.Type type = field.schema().type();
        final String fieldName = field.name();
        final Object fieldValue = record.get(fieldName);
       // System.out.println("fieldValue === "+ (fieldValue != null ? fieldValue : " null field value for "+fieldName));
        if(fieldValue != null){
        switch (type) {
            case STRING:
                return Bytes.toBytes((String) fieldValue);
            case BOOLEAN:
                return Bytes.toBytes((Boolean)fieldValue);
            case BYTES:
                return Bytes.toBytes((ByteBuffer) fieldValue);
            case FLOAT32:
                return Bytes.toBytes((Float)fieldValue);
            case FLOAT64:
                return Bytes.toBytes((Double)fieldValue);
            case INT8:
                return Bytes.toBytes((Byte)fieldValue);
            case INT16:
                return Bytes.toBytes((Short)fieldValue);
            case INT32:
                return Bytes.toBytes((Integer)fieldValue);
            case INT64:
                return Bytes.toBytes((Long)fieldValue);
            default:
                return null;
        }
        }return null;
    }
}
