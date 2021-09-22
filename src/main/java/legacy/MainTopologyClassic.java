package legacy;
import org.apache.storm.Config;
//import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopologyClassic {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        // builder.setSpout("IntegerSpout", new IntegerSpout());
        builder.setSpout("IntegerSpout", new IntegerSpout(), 5);
        builder.setBolt("MyBolt", new MyBolt(), 2).shuffleGrouping("IntegerSpout");

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(8);
        //config.setMaxSpoutPending(50000);
        config.setNumEventLoggers(1);
        config.setStatsSampleRate(0.1);
        config.setNumAckers(1);

        // bei local
        // LocalCluster cluster = new LocalCluster();

        try {
            // for local cluster:
            // cluster.submitTopology("topology", config, builder.createTopology());
            // for production cluster:
            StormSubmitter.submitTopology("mytopology", config, builder.createTopology());
            // Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // cluster.shutdown();
        }
    }
}
