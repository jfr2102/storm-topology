import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
//import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.ProcessingGuarantee;

public class MainTopology {
    public static void main(String[] args) throws Exception {
    final TopologyBuilder tp = new TopologyBuilder();
    KafkaSpoutConfig kafkaConfig = KafkaSpoutConfig.builder("kafka:9094", "mytopic")
                                    .setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE)
                                    //.setFirstPollOffsetStrategy(firstPollOffsetStrategy)
                                    .setProp(ConsumerConfig.GROUP_ID_CONFIG, "stormconsumer")
                                    .build();
    tp.setSpout("kafka_spout", new KafkaSpout<>(kafkaConfig), 2);

    tp.setBolt("bolt", new KafkaBolt(), 4).shuffleGrouping("kafka_spout");

       // builder.setSpout("kafka_spout", new KafkaSpout<>(KafkaSpoutConfig.builder("localhost:9092", "mytopic").build()), 1);
        //builder.setBolt("bolt", new myBolt()).shuffleGrouping("kafka_spout");

        // kafkaSpout = new
        // KafkaSpout<>(KafkaSpoutConfig.builder("localhost:9092","mytopic").build());
        // builder.setSpout("kafka_spout", kafkaSpout);

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(8);
        // config.setMaxSpoutPending(50000);
        config.setNumEventLoggers(1);
        config.setStatsSampleRate(0.1);
        config.setNumAckers(1);
        try {
            StormSubmitter.submitTopology("KafkaTopology", config, tp.createTopology());
            // Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // cluster.shutdown();
        }
    }
}
// java.lang.RuntimeException: org.apache.kafka.common.errors.TimeoutException: Timeout of 60000ms expired before the last committed offset for partitions [mytopic-0] could be determined. Try tuning default.api.timeout.ms larger to relax the threshold.
// at org.apache.storm.utils.Utils$1.run(Utils.java:409)
// at java.base/java.lang.Thread.run(Unknown Source)
// Caused by: org.apache.kafka.common.errors.TimeoutException: Timeout of 60000ms expired before the last committed offset for partitions [mytopic-0] could be determined. Try tuning default.api.timeout.ms larger to relax the threshold.