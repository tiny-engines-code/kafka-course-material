//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.avro;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.generic.GenericData.Fixed;

public class Conversions {
    public Conversions() {
    }

    public static Object convertToLogicalType(Object datum, Schema schema, LogicalType type, Conversion<?> conversion) {
        if (datum == null) {
            return null;
        } else if (schema != null && type != null && conversion != null) {
            try {
                switch(schema.getType()) {
                case RECORD:
                    return conversion.fromRecord((IndexedRecord)datum, schema, type);
                case ENUM:
                    return conversion.fromEnumSymbol((GenericEnumSymbol)datum, schema, type);
                case ARRAY:
                    return conversion.fromArray((Collection)datum, schema, type);
                case MAP:
                    return conversion.fromMap((Map)datum, schema, type);
                case FIXED:
                    return conversion.fromFixed((GenericFixed)datum, schema, type);
                case STRING:
                    return conversion.fromCharSequence((CharSequence)datum, schema, type);
                case BYTES:
                    return conversion.fromBytes((ByteBuffer)datum, schema, type);
                case INT:
                    return conversion.fromInt((Integer)datum, schema, type);
                case LONG:
                    return conversion.fromLong((Long)datum, schema, type);
                case FLOAT:
                    return conversion.fromFloat((Float)datum, schema, type);
                case DOUBLE:
                    return conversion.fromDouble((Double)datum, schema, type);
                case BOOLEAN:
                    return conversion.fromBoolean((Boolean)datum, schema, type);
                default:
                    return datum;
                }
            } catch (ClassCastException var5) {
                throw new AvroRuntimeException("Cannot convert " + datum + ":" + datum.getClass().getSimpleName() + ": expected generic type", var5);
            }
        } else {
            throw new IllegalArgumentException("Parameters cannot be null! Parameter values:" + Arrays.deepToString(new Object[]{datum, schema, type, conversion}));
        }
    }


    /*
    CHRIS4
    This is going to convert my Instance to a Long using return timestamp.toEpochMilli();
    datum = Instant.class = 2020-09-09T18:42:19.317Z
    schema = Schema$LongSchema.class = {"type":"long","logicalType":"timestamp-millis"}
    type = LogicalType = timestamp-millis
    conversion = TimeConversions$TimestampMillisConversion from NotificationStatus
    */
    public static <T> Object convertToRawType(Object datum, Schema schema, LogicalType type, Conversion<T> conversion) {
        if (datum == null) {
            return null;
        } else if (schema != null && type != null && conversion != null) {
            try {
                Class<T> fromClass = conversion.getConvertedType();
                switch(schema.getType()) {
                case RECORD:
                    return conversion.toRecord(fromClass.cast(datum), schema, type);
                case ENUM:
                    return conversion.toEnumSymbol(fromClass.cast(datum), schema, type);
                case ARRAY:
                    return conversion.toArray(fromClass.cast(datum), schema, type);
                case MAP:
                    return conversion.toMap(fromClass.cast(datum), schema, type);
                case FIXED:
                    return conversion.toFixed(fromClass.cast(datum), schema, type);
                case STRING:
                    return conversion.toCharSequence(fromClass.cast(datum), schema, type);
                case BYTES:
                    return conversion.toBytes(fromClass.cast(datum), schema, type);
                case INT:
                    return conversion.toInt(fromClass.cast(datum), schema, type);
                case LONG:
                    return conversion.toLong(fromClass.cast(datum), schema, type);
                case FLOAT:
                    return conversion.toFloat(fromClass.cast(datum), schema, type);
                case DOUBLE:
                    return conversion.toDouble(fromClass.cast(datum), schema, type);
                case BOOLEAN:
                    return conversion.toBoolean(fromClass.cast(datum), schema, type);
                default:
                    return datum;
                }
            } catch (ClassCastException var5) {
                throw new AvroRuntimeException("Cannot convert " + datum + ":" + datum.getClass().getSimpleName() + ": expected logical type", var5);
            }
        } else {
            throw new IllegalArgumentException("Parameters cannot be null! Parameter values:" + Arrays.deepToString(new Object[]{datum, schema, type, conversion}));
        }
    }

    public static class DecimalConversion extends Conversion<BigDecimal> {
        public DecimalConversion() {
        }

        public Class<BigDecimal> getConvertedType() {
            return BigDecimal.class;
        }

        public Schema getRecommendedSchema() {
            throw new UnsupportedOperationException("No recommended schema for decimal (scale is required)");
        }

        public String getLogicalTypeName() {
            return "decimal";
        }

        public BigDecimal fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
            int scale = ((Decimal)type).getScale();
            byte[] bytes = new byte[value.remaining()];
            value.duplicate().get(bytes);
            return new BigDecimal(new BigInteger(bytes), scale);
        }

        public ByteBuffer toBytes(BigDecimal value, Schema schema, LogicalType type) {
            value = validate((Decimal)type, value);
            return ByteBuffer.wrap(value.unscaledValue().toByteArray());
        }

        public BigDecimal fromFixed(GenericFixed value, Schema schema, LogicalType type) {
            int scale = ((Decimal)type).getScale();
            return new BigDecimal(new BigInteger(value.bytes()), scale);
        }

        public GenericFixed toFixed(BigDecimal value, Schema schema, LogicalType type) {
            value = validate((Decimal)type, value);
            byte fillByte = (byte)(value.signum() < 0 ? 255 : 0);
            byte[] unscaled = value.unscaledValue().toByteArray();
            byte[] bytes = new byte[schema.getFixedSize()];
            int offset = bytes.length - unscaled.length;
            Arrays.fill(bytes, 0, offset, fillByte);
            System.arraycopy(unscaled, 0, bytes, offset, bytes.length - offset);
            return new Fixed(schema, bytes);
        }

        private static BigDecimal validate(final Decimal decimal, BigDecimal value) {
            int scale = decimal.getScale();
            int valueScale = value.scale();
            boolean scaleAdjusted = false;
            if (valueScale != scale) {
                try {
                    value = value.setScale(scale, 7);
                    scaleAdjusted = true;
                } catch (ArithmeticException var7) {
                    throw new AvroTypeException("Cannot encode decimal with scale " + valueScale + " as scale " + scale + " without rounding");
                }
            }

            int precision = decimal.getPrecision();
            int valuePrecision = value.precision();
            if (valuePrecision > precision) {
                if (scaleAdjusted) {
                    throw new AvroTypeException("Cannot encode decimal with precision " + valuePrecision + " as max precision " + precision + ". This is after safely adjusting scale from " + valueScale + " to required " + scale);
                } else {
                    throw new AvroTypeException("Cannot encode decimal with precision " + valuePrecision + " as max precision " + precision);
                }
            } else {
                return value;
            }
        }
    }

    public static class UUIDConversion extends Conversion<UUID> {
        public UUIDConversion() {
        }

        public Class<UUID> getConvertedType() {
            return UUID.class;
        }

        public Schema getRecommendedSchema() {
            return LogicalTypes.uuid().addToSchema(Schema.create(Type.STRING));
        }

        public String getLogicalTypeName() {
            return "uuid";
        }

        public UUID fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
            return UUID.fromString(value.toString());
        }

        public CharSequence toCharSequence(UUID value, Schema schema, LogicalType type) {
            return value.toString();
        }
    }
}
