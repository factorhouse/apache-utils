/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This is a modified version of tech.allegro.schema.json2avro.converter.JsonGenericRecordReader
 * This class includes further source from
 *  - tech.allegro.schema.json2avro.converter.PathsPrinter, and
 *  - tech.allegro.schema.json2avro.converter.AvroTypeExceptions
 */

package io.kpow.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import tech.allegro.schema.json2avro.converter.AvroConversionException;
import tech.allegro.schema.json2avro.converter.UnknownFieldListener;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class KpowJsonGenericRecordReader {
    private static final Object INCOMPATIBLE = new Object();
    private final ObjectMapper mapper;
    private final UnknownFieldListener unknownFieldListener;

    public KpowJsonGenericRecordReader() {
        this(new ObjectMapper());
    }

    public KpowJsonGenericRecordReader(ObjectMapper mapper) {
        this(mapper, null);
    }

    public KpowJsonGenericRecordReader(ObjectMapper mapper, UnknownFieldListener unknownFieldListener) {
        this.mapper = mapper;
        this.unknownFieldListener = unknownFieldListener;
    }

    @SuppressWarnings("unchecked")
    public GenericData.Record read(byte[] data, Schema schema) {
        try {
            return read(mapper.readValue(data, Map.class), schema);
        } catch (IOException ex) {
            throw new AvroConversionException("Failed to parse json to map format.", ex);
        }
    }

    public GenericData.Record read(Map<String, Object> json, Schema schema) {
        Deque<String> path = new ArrayDeque<>();
        try {
            return readRecord(json, schema, path);
        } catch (AvroTypeException ex) {
            throw new AvroConversionException("Failed to convert JSON to Avro: " + ex.getMessage(), ex);
        } catch (AvroRuntimeException ex) {
            throw new AvroConversionException("Failed to convert JSON to Avro", ex);
        }
    }

    private GenericData.Record readRecord(Map<String, Object> json, Schema schema, Deque<String> path) {
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        json.entrySet().forEach(entry -> {
            Schema.Field field = schema.getField(entry.getKey());
            if (field != null) {
                record.set(field, read(field, field.schema(), entry.getValue(), path, false));
            } else if (unknownFieldListener != null) {
                unknownFieldListener.onUnknownField(entry.getKey(), entry.getValue(), "");
            }
        });
        return record.build();
    }

    @SuppressWarnings("unchecked")
    private Object read(Schema.Field field, Schema schema, Object value, Deque<String> path, boolean silently) {
        boolean pushed = !field.name().equals(path.peekLast());
        if (pushed) {
            path.addLast(field.name());
        }
        Object result;

        switch (schema.getType()) {
            case RECORD:
                result = onValidType(value, Map.class, path, silently, map -> readRecord(map, schema, path));
                break;
            case ARRAY:
                result = onValidType(value, List.class, path, silently, list -> readArray(field, schema, list, path));
                break;
            case MAP:
                result = onValidType(value, Map.class, path, silently, map -> readMap(field, schema, map, path));
                break;
            case UNION:
                result = readUnion(field, schema, value, path);
                break;
            case INT:
                result = onValidNumber(value, path, silently, Number::intValue);
                break;
            case LONG:
                result = onValidNumber(value, path, silently, Number::longValue);
                break;
            case FLOAT:
                result = onValidNumber(value, path, silently, Number::floatValue);
                break;
            case DOUBLE:
                result = onValidNumber(value, path, silently, Number::doubleValue);
                break;
            case BOOLEAN:
                result = onValidType(value, Boolean.class, path, silently, bool -> bool);
                break;
            case ENUM:
                result = onValidType(value, String.class, path, silently, string -> ensureEnum(schema, string, path));
                break;
            case STRING:
                result = onValidType(value, String.class, path, silently, string -> string);
                break;
            case BYTES:
                result = onValidBytesType(schema, value, String.class, path, silently, string -> bytesForString(string));
                break;
            case NULL:
                result = value == null ? value : INCOMPATIBLE;
                break;
            default:
                throw new AvroTypeException("Unsupported type: " + field.schema().getType());
        }

        if (pushed) {
            path.removeLast();
        }
        return result;
    }

    private List<Object> readArray(Schema.Field field, Schema schema, List<Object> items, Deque<String> path) {
        return items.stream().map(item -> read(field, schema.getElementType(), item, path, false)).collect(toList());
    }

    private Map<String, Object> readMap(Schema.Field field, Schema schema, Map<String, Object> map, Deque<String> path) {
        Map<String, Object> result = new HashMap<>(map.size());
        map.forEach((k, v) -> result.put(k, read(field, schema.getValueType(), v, path, false)));
        return result;
    }

    private Object readUnion(Schema.Field field, Schema schema, Object value, Deque<String> path) {
        List<Schema> types = schema.getTypes();
        for (Schema type : types) {
            try {
                Object nestedValue = read(field, type, value, path, true);
                if (nestedValue == INCOMPATIBLE) {
                    continue;
                } else {
                    return nestedValue;
                }
            } catch (AvroRuntimeException e) {
                // thrown only for union of more complex types like records
                continue;
            }
        }
        throw unionException(
                field.name(),
                types.stream().map(Schema::getType).map(Object::toString).collect(joining(", ")),
                path);
    }

    private Object ensureEnum(Schema schema, Object value, Deque<String> path) {
        List<String> symbols = schema.getEnumSymbols();
        if (symbols.contains(value)) {
            return new GenericData.EnumSymbol(schema, value);
        }
        throw enumException(path, symbols.stream().map(String::valueOf).collect(joining(", ")));
    }

    private ByteBuffer bytesForString(String string) {
        return ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8));
    }

    @SuppressWarnings("unchecked")
    public <T> Object onValidType(Object value, Class<T> type, Deque<String> path, boolean silently, Function<T, Object> function)
            throws AvroTypeException {

        if (type.isInstance(value)) {
            return function.apply((T) value);
        } else {
            if (silently) {
                return INCOMPATIBLE;
            } else {
                throw typeException(path, type.getTypeName());
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <T> Object onValidBytesType(Schema schema, Object value, Class<T> type, Deque<String> path, boolean silently, Function<T, Object> function)
            throws AvroTypeException {

        LogicalType dataType = schema.getLogicalType();

        if (type.isInstance(value)) {
            if (dataType != null && "decimal".equals(dataType.getName())) {
                Conversion<String> con = new KpowDecimalInputConversion();
                String dec = con.fromBytes((ByteBuffer) function.apply((T) value), schema, dataType);
                return con.toBytes(dec, schema, dataType);
            } else {
                return function.apply((T) value);
            }
        } else {
            if (silently) {
                return INCOMPATIBLE;
            } else {
                throw typeException(path, type.getTypeName());
            }
        }
    }

    public Object onValidNumber(Object value, Deque<String> path, boolean silently, Function<Number, Object> function) {
        return onValidType(value, Number.class, path, silently, function);
    }

    static AvroTypeException enumException(Deque<String> fieldPath, String expectedSymbols) {
        return new AvroTypeException(new StringBuilder()
                .append("Field ")
                .append(print(fieldPath))
                .append(" is expected to be of enum type and be one of ")
                .append(expectedSymbols)
                .toString());
    }

    static AvroTypeException unionException(String fieldName, String expectedTypes, Deque<String> offendingPath) {
        return new AvroTypeException(new StringBuilder()
                .append("Could not evaluate union, field ")
                .append(fieldName)
                .append(" is expected to be one of these: ")
                .append(expectedTypes)
                .append(". If this is a complex type, check if offending field: ")
                .append(print(offendingPath))
                .append(" adheres to schema.")
                .toString());
    }

    static AvroTypeException typeException(Deque<String> fieldPath, String expectedType) {
        return new AvroTypeException(new StringBuilder()
                .append("Field ")
                .append(print(fieldPath))
                .append(" is expected to be type: ")
                .append(expectedType)
                .toString());
    }

    static String print(Deque<String> path) {
        return path.stream().collect(joining("."));
    }

    static String print(Deque<String> path, String additionalSegment) {
        if (path.isEmpty()) {
            return additionalSegment;
        }
        return print(path) + "." + additionalSegment;
    }
}
