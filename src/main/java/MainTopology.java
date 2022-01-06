import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
//import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.ProcessingGuarantee;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;

public class MainTopology {
    public static void main(String[] args) throws Exception {
        final TopologyBuilder tp = new TopologyBuilder();
        KafkaSpoutConfig kafkaConfig = KafkaSpoutConfig.builder("kafka:9094", "mytopic")
                .setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE)
                // .setFirstPollOffsetStrategy(firstPollOffsetStrategy)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "stormconsumer")
                // .setRecordTranslator(func, fields)
                .setRecordTranslator(new KafkaRecordTranslator<String, String>()).build();
        tp.setSpout("kafka_spout", new KafkaSpout<>(kafkaConfig), 6);

        tp.setBolt("bolt", new KafkaParserBolt(), 6).fieldsGrouping("kafka_spout", new Fields("partition"));

       /* tp.setBolt("windowbolt",
                new SlidingWindowBolt()
                        .withTimestampField("timestamp")
                        .withTumblingWindow(new Duration(5, TimeUnit.SECONDS))
                        .withLag(new Duration(100, TimeUnit.MILLISECONDS))
                        .withLateTupleStream("late_tuples")
                ,6).fieldsGrouping("bolt", new Fields("partition"));
       */
      
        tp.setBolt("windowbolt", new StatefulWindowBolt()
                        .withTimestampField("timestamp")
                        .withTumblingWindow(new Duration(5, TimeUnit.SECONDS))
                        .withLag(new Duration(100, TimeUnit.MILLISECONDS))
                        .withPersistence()
                        .withLateTupleStream("late_tuples")
                ,6).fieldsGrouping("bolt", new Fields("partition"));

        Properties kafkaExportProps = new Properties();
        kafkaExportProps.put("bootstrap.servers", "kafka:9094");
        kafkaExportProps.put("acks", "1");
        kafkaExportProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaExportProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt kafkaBolt = new KafkaBolt().withProducerProperties(kafkaExportProps).withTopicSelector("results")
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
        tp.setBolt("Kafka_Emitter", kafkaBolt, 6).localOrShuffleGrouping("windowbolt");
        tp.setBolt("Late_Tuple_bolt", new LateTupleBolt()).shuffleGrouping("windowbolt", "late_tuples");

        Config config = new Config();
        config.setMaxSpoutPending(500000);
        config.setDebug(false);
        config.setNumWorkers(24);//24
        config.setFallBackOnJavaSerialization(true);
        config.registerSerialization(AvgState.class);
        config.put(Config.TOPOLOGY_MIN_REPLICATION_COUNT, 1);
        config.put(Config.TOPOLOGY_STATE_PROVIDER, "org.apache.storm.redis.state.RedisKeyValueStateProvider");
        config.put(Config.TOPOLOGY_STATE_PROVIDER_CONFIG, "{\"jedisPoolConfig\":{\"host\":\"redis\", \"port\":6379}}");
        // config.setStatsSampleRate(0.01);
        // config.setNumEventLoggers(2);
        // config.setNumAckers(2); 
        try {
            StormSubmitter.submitTopology("KafkaTopology", config, tp.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        
        }
    }
}