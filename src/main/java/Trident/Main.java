package Trident;


import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutConfig;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaStateUpdater;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.hbase.trident.windowing.HBaseWindowsStoreFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class Main {
    public static void main(String[] args) throws UnsupportedEncodingException {
        final TridentTopology tridentTopology = new TridentTopology();

        final Stream spoutStream = tridentTopology.newStream("kafkaSpout",
                        new KafkaTridentSpoutOpaque<>(KafkaTridentSpoutConfig.builder("localhost:9094", Pattern.compile("mytopic")).setRecordTranslator(new KafkaRecordTranslator<String, String>()).build()))
                .parallelismHint(4);


        HBaseWindowsStoreFactory windowStoreFactory = new HBaseWindowsStoreFactory(new HashMap<String, Object>(), "window-state", "cf".getBytes("UTF-8"), "tuples".getBytes("UTF-8"));

        KafkaTridentParser kafkaParser = new KafkaTridentParser();
        spoutStream
                .map(new KafkaTridentParser(), kafkaParser.getOutputFields())
                .tumblingWindow(10000, windowStoreFactory, kafkaParser.getOutputFields(), new WindowAverage(), WindowAverage.getOutPutFields());

        //set producer properties.
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9094");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withProducerProperties(props)
                .withKafkaTopicSelector(new DefaultTopicSelector("results"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "value"));
        spoutStream.partitionPersist(stateFactory, new Fields("TODO"), new TridentKafkaStateUpdater(), new Fields());


    }
}