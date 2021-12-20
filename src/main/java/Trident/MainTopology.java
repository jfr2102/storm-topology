package Trident;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutConfig;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaStateUpdater;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.hbase.trident.windowing.HBaseWindowsStoreFactory;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

public class MainTopology {
    public static void main(String[] args) throws UnsupportedEncodingException, AuthorizationException, InvalidTopologyException, AlreadyAliveException {
        final TridentTopology tridentTopology = new TridentTopology();
        // Read from Kafka
        final Stream spoutStream = tridentTopology.newStream("kafkaSpout",
                        new KafkaTridentSpoutOpaque<>(KafkaTridentSpoutConfig.builder("kafka:9094", Pattern.compile("mytopic")).setRecordTranslator(new KafkaRecordTranslator<String, String>()).build()))
                .parallelismHint(2);

        // Parse Kafka events and query over window
        HBaseWindowsStoreFactory windowStoreFactory = new HBaseWindowsStoreFactory(new HashMap<String, Object>(), "window-state", "cf".getBytes("UTF-8"), "tuples".getBytes("UTF-8"));
        //windowStoreFactory.
        KafkaTridentParser kafkaParser = new KafkaTridentParser();
        spoutStream
                .map(new KafkaTridentParser(), kafkaParser.getOutputFields())
                .tumblingWindow(10000, windowStoreFactory, kafkaParser.getOutputFields(), new WindowAverage(), WindowAverage.getOutPutFields())
                .parallelismHint(2);

        //set Kafka producer properties.
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9094");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //send query results to Kafka
        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withProducerProperties(props)
                .withKafkaTopicSelector(new DefaultTopicSelector("results"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "value"));
        spoutStream.partitionPersist(stateFactory, WindowAverage.getOutPutFields(), new TridentKafkaStateUpdater(), new Fields());

        // build config and topology
        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(6);
        //config.setMaxSpoutPending(100000);
        config.setNumEventLoggers(1);
        config.setStatsSampleRate(0.01);
        config.setNumAckers(1);
        //config.put("storm.zookeeper.servers", "[172.24.38.175]");
        StormSubmitter.submitTopology("kafkaTrident", config, tridentTopology.build());

    }
}