//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.avro.specific;

import java.io.IOException;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

public class SpecificDatumWriter<T> extends GenericDatumWriter<T> {
    public SpecificDatumWriter() {
        super(SpecificData.get());
    }

    public SpecificDatumWriter(Class<T> c) {
        super(SpecificData.get().getSchema(c), SpecificData.getForClass(c));
    }

    public SpecificDatumWriter(Schema schema) {
        super(schema, SpecificData.getForSchema(schema));
    }

    public SpecificDatumWriter(Schema root, SpecificData specificData) {
        super(root, specificData);
    }

    protected SpecificDatumWriter(SpecificData specificData) {
        super(specificData);
    }

    public SpecificData getSpecificData() {
        return (SpecificData)this.getData();
    }

    protected void writeEnum(Schema schema, Object datum, Encoder out) throws IOException {
        if (!(datum instanceof Enum)) {
            super.writeEnum(schema, datum, out);
        } else {
            out.writeEnum(((Enum)datum).ordinal());
        }

    }

    protected void writeString(Schema schema, Object datum, Encoder out) throws IOException {
        if (!(datum instanceof CharSequence) && this.getSpecificData().isStringable(datum.getClass())) {
            datum = datum.toString();
        }

        this.writeString(datum, out);
    }

    protected void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
        if (datum instanceof SpecificRecordBase && this.getSpecificData().useCustomCoders()) {
            SpecificRecordBase d = (SpecificRecordBase)datum;
            if (d.hasCustomCoders()) {
                d.customEncode(out);
                return;
            }
        }

        super.writeRecord(schema, datum, out);
    }

    // CHRIS1 - this is the logic
    protected void writeField(Object datum, Field f, Encoder out, Object state) throws IOException {
        if (datum instanceof SpecificRecordBase) {
            Conversion<?> conversion = ((SpecificRecordBase)datum).getConversion(f.pos());  // in NotificationStatus - not null for timestamp stuff fom NotificationStatus - it's  new org.apache.avro.data.TimeConversions.TimestampMillisConversion(),
            Schema fieldSchema = f.schema();
            LogicalType logicalType = fieldSchema.getLogicalType();
            Object value = this.getData().getField(datum, f.name(), f.pos());
            if (conversion != null && logicalType != null) {
                value = this.convert(fieldSchema, logicalType, conversion, value);  // in GenericDatumWriter eventually going to convert my Instant to a Long
            }

            this.writeWithoutConversion(fieldSchema, value, out);
        } else {
            super.writeField(datum, f, out, state);
        }

    }
}
