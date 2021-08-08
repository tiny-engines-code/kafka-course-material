//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.avro.generic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

public class GenericDatumWriter<D> implements DatumWriter<D> {
    private final GenericData data;
    private Schema root;

    public GenericDatumWriter() {
        this(GenericData.get());
    }

    protected GenericDatumWriter(GenericData data) {
        this.data = data;
    }

    public GenericDatumWriter(Schema root) {
        this();
        this.setSchema(root);
    }

    public GenericDatumWriter(Schema root, GenericData data) {
        this(data);
        this.setSchema(root);
    }

    public GenericData getData() {
        return this.data;
    }

    public void setSchema(Schema root) {
        this.root = root;
    }

    public void write(D datum, Encoder out) throws IOException {
        Objects.requireNonNull(out, "Encoder cannot be null");
        this.write(this.root, datum, out);
    }

    protected void write(Schema schema, Object datum, Encoder out) throws IOException {
        LogicalType logicalType = schema.getLogicalType();
        if (datum != null && logicalType != null) {
            Conversion<?> conversion = this.getData().getConversionByClass(datum.getClass(), logicalType);
            this.writeWithoutConversion(schema, this.convert(schema, logicalType, conversion, datum), out);
        } else {
            this.writeWithoutConversion(schema, datum, out);
        }

    }

    // CHRIS2 - we have a timestamp and are converting it
    // datum is an Instant Object
    // schema is {"type":"long","logicalType":"timestamp-millis"}
    // Logical Type is LogicalTypes.name = "timestamp-millis"
    protected <T> Object convert(Schema schema, LogicalType logicalType, Conversion<T> conversion, Object datum) {
        try {
            return conversion == null ? datum : Conversions.convertToRawType(datum, schema, logicalType, conversion);  // org.apache.avro.Conversions
        } catch (AvroRuntimeException var7) {
            Throwable cause = var7.getCause();
            if (cause != null && cause.getClass() == ClassCastException.class) {
                throw (ClassCastException)cause;
            } else {
                throw var7;
            }
        }
    }

    protected void writeWithoutConversion(Schema schema, Object datum, Encoder out) throws IOException {
        try {
            switch(schema.getType()) {
            case RECORD:
                this.writeRecord(schema, datum, out);
                break;
            case ENUM:
                this.writeEnum(schema, datum, out);
                break;
            case ARRAY:
                this.writeArray(schema, datum, out);
                break;
            case MAP:
                this.writeMap(schema, datum, out);
                break;
            case UNION:
                int index = this.resolveUnion(schema, datum);
                out.writeIndex(index);
                this.write((Schema)schema.getTypes().get(index), datum, out);
                break;
            case FIXED:
                this.writeFixed(schema, datum, out);
                break;
            case STRING:
                this.writeString(schema, datum, out);
                break;
            case BYTES:
                this.writeBytes(datum, out);
                break;
            case INT:
                out.writeInt(((Number)datum).intValue());
                break;
            case LONG:
                out.writeLong(((Number)datum).longValue());
                break;
            case FLOAT:
                out.writeFloat(((Number)datum).floatValue());
                break;
            case DOUBLE:
                out.writeDouble(((Number)datum).doubleValue());
                break;
            case BOOLEAN:
                out.writeBoolean((Boolean)datum);
                break;
            case NULL:
                out.writeNull();
                break;
            default:
                this.error(schema, datum);
            }

        } catch (NullPointerException var5) {
            throw this.npe(var5, " of " + schema.getFullName());
        }
    }

    protected NullPointerException npe(NullPointerException e, String s) {
        NullPointerException result = new NullPointerException(e.getMessage() + s);
        result.initCause((Throwable)(e.getCause() == null ? e : e.getCause()));
        return result;
    }

    protected ClassCastException addClassCastMsg(ClassCastException e, String s) {
        ClassCastException result = new ClassCastException(e.getMessage() + s);
        result.initCause((Throwable)(e.getCause() == null ? e : e.getCause()));
        return result;
    }

    protected AvroTypeException addAvroTypeMsg(AvroTypeException e, String s) {
        AvroTypeException result = new AvroTypeException(e.getMessage() + s);
        result.initCause((Throwable)(e.getCause() == null ? e : e.getCause()));
        return result;
    }

    protected void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
        Object state = this.data.getRecordState(datum, schema);
        Iterator var5 = schema.getFields().iterator();

        while(var5.hasNext()) {
            Field f = (Field)var5.next();
            this.writeField(datum, f, out, state);
        }

    }

    protected void writeField(Object datum, Field f, Encoder out, Object state) throws IOException {
        Object value = this.data.getField(datum, f.name(), f.pos(), state);

        try {
            this.write(f.schema(), value, out);
        } catch (UnresolvedUnionException var8) {
            UnresolvedUnionException unresolvedUnionException = new UnresolvedUnionException(f.schema(), f, value);
            unresolvedUnionException.addSuppressed(var8);
            throw unresolvedUnionException;
        } catch (NullPointerException var9) {
            throw this.npe(var9, " in field " + f.name());
        } catch (ClassCastException var10) {
            throw this.addClassCastMsg(var10, " in field " + f.name());
        } catch (AvroTypeException var11) {
            throw this.addAvroTypeMsg(var11, " in field " + f.name());
        }
    }

    protected void writeEnum(Schema schema, Object datum, Encoder out) throws IOException {
        if (!this.data.isEnum(datum)) {
            throw new AvroTypeException("Not an enum: " + datum + " for schema: " + schema);
        } else {
            out.writeEnum(schema.getEnumOrdinal(datum.toString()));
        }
    }

    protected void writeArray(Schema schema, Object datum, Encoder out) throws IOException {
        Schema element = schema.getElementType();
        long size = this.getArraySize(datum);
        long actualSize = 0L;
        out.writeArrayStart();
        out.setItemCount(size);

        for(Iterator it = this.getArrayElements(datum); it.hasNext(); ++actualSize) {
            out.startItem();
            this.write(element, it.next(), out);
        }

        out.writeArrayEnd();
        if (actualSize != size) {
            throw new ConcurrentModificationException("Size of array written was " + size + ", but number of elements written was " + actualSize + ". ");
        }
    }

    protected int resolveUnion(Schema union, Object datum) {
        return this.data.resolveUnion(union, datum);
    }

    protected long getArraySize(Object array) {
        return (long)((Collection)array).size();
    }

    protected Iterator<? extends Object> getArrayElements(Object array) {
        return ((Collection)array).iterator();
    }

    protected void writeMap(Schema schema, Object datum, Encoder out) throws IOException {
        Schema value = schema.getValueType();
        int size = this.getMapSize(datum);
        int actualSize = 0;
        out.writeMapStart();
        out.setItemCount((long)size);

        for(Iterator var7 = this.getMapEntries(datum).iterator(); var7.hasNext(); ++actualSize) {
            Entry<Object, Object> entry = (Entry)var7.next();
            out.startItem();
            this.writeString(entry.getKey().toString(), out);
            this.write(value, entry.getValue(), out);
        }

        out.writeMapEnd();
        if (actualSize != size) {
            throw new ConcurrentModificationException("Size of map written was " + size + ", but number of entries written was " + actualSize + ". ");
        }
    }

    protected int getMapSize(Object map) {
        return ((Map)map).size();
    }

    protected Iterable<Entry<Object, Object>> getMapEntries(Object map) {
        return ((Map)map).entrySet();
    }

    protected void writeString(Schema schema, Object datum, Encoder out) throws IOException {
        this.writeString(datum, out);
    }

    protected void writeString(Object datum, Encoder out) throws IOException {
        out.writeString((CharSequence)datum);
    }

    protected void writeBytes(Object datum, Encoder out) throws IOException {
        out.writeBytes((ByteBuffer)datum);
    }

    protected void writeFixed(Schema schema, Object datum, Encoder out) throws IOException {
        out.writeFixed(((GenericFixed)datum).bytes(), 0, schema.getFixedSize());
    }

    private void error(Schema schema, Object datum) {
        throw new AvroTypeException("Not a " + schema + ": " + datum);
    }
}
