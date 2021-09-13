import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
public class KafkaRecordTranslator<K, V> implements RecordTranslator<K, V> {
    private static final long serialVersionUID = -5782462870112305750L;
    public static final Fields FIELDS = new Fields("topic", "partition", "offset", "key", "value", "timestamp");

    @Override
    public List<Object> apply(ConsumerRecord<K, V> record) {
        return new Values(record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value(),
                        record.timestamp());
    }

    @Override
    public Fields getFieldsFor(String stream) {
        return FIELDS;
    }
}
