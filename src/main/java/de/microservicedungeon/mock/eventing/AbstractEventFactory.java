package de.microservicedungeon.mock.eventing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.ByteBuffer;

@AllArgsConstructor
public abstract class AbstractEventFactory<T> {
    private final ObjectMapper objectMapper;
    private final SequenceIdManager sequenceIdManager;

    protected ProducerRecord<String, byte[]> buildRecord(String topic, String key, T payload, CommonHeaders headers){

        ProducerRecord<String, byte[]> record;
        try {
            record = new ProducerRecord<>(
                    topic,
                    key,
                    objectMapper.writeValueAsBytes(payload)
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        headers.toKafkaHeaders().forEach(record.headers()::add);
        record.headers().add(new RecordHeader("sequence_id", ByteBuffer.allocate(Long.BYTES).putLong(sequenceIdManager.getNextSequenceId(topic)).array()));
        return record;
    }
}