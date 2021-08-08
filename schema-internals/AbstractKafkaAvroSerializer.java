//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package io.confluent.kafka.serializers;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;

public abstract class AbstractKafkaAvroSerializer extends AbstractKafkaAvroSerDe {
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    protected boolean autoRegisterSchema;

    public AbstractKafkaAvroSerializer() {
    }

    protected void configure(KafkaAvroSerializerConfig config) {
        this.configureClientProperties(config);
        this.autoRegisterSchema = config.autoRegisterSchema();
    }

    protected KafkaAvroSerializerConfig serializerConfig(Map<String, ?> props) {
        return new KafkaAvroSerializerConfig(props);
    }

    protected KafkaAvroSerializerConfig serializerConfig(VerifiableProperties props) {
        return new KafkaAvroSerializerConfig(props.props());
    }

    protected byte[] serializeImpl(String subject, Object object) throws SerializationException {
        Schema schema = null;
        if (object == null) {
            return null;
        } else {
            String restClientErrorMsg = "";

            try {
                schema = AvroSchemaUtils.getSchema(object);
                int id;
                if (this.autoRegisterSchema) {
                    restClientErrorMsg = "Error registering Avro schema: ";
                    id = this.schemaRegistry.register(subject, schema);
                } else {
                    restClientErrorMsg = "Error retrieving Avro schema: ";
                    id = this.schemaRegistry.getId(subject, schema);
                }

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                out.write(0);
                out.write(ByteBuffer.allocate(4).putInt(id).array());
                if (object instanceof byte[]) {
                    out.write((byte[])((byte[])object));
                } else {
                    BinaryEncoder encoder = this.encoderFactory.directBinaryEncoder(out, (BinaryEncoder)null);
                    Object value = object instanceof NonRecordContainer ? ((NonRecordContainer)object).getValue() : object;
                    Object writer;
                    if (value instanceof SpecificRecord) {
                        writer = new SpecificDatumWriter(schema);
                    } else {
                        writer = new GenericDatumWriter(schema);
                    }

                    ((DatumWriter)writer).write(value, encoder);
                    encoder.flush();
                }

                byte[] bytes = out.toByteArray();
                out.close();
                return bytes;
            } catch (RuntimeException | IOException var10) {
                throw new SerializationException("Error serializing Avro message", var10);
            } catch (RestClientException var11) {
                throw new SerializationException(restClientErrorMsg + schema, var11);
            }
        }
    }
}
