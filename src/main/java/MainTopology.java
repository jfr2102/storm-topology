import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("IntegerSpout", new IntegerSpout());
        builder.setBolt("MyBolt",new MyBolt()).shuffleGrouping("IntegerSpout");

        Config config = new Config();
        config.setDebug(false);
        //config.setNumWorkers(10);
        //config.setMaxSpoutPending(5000);
        LocalCluster cluster = new LocalCluster();

        try{
            cluster.submitTopology("toplogy", config, builder.createTopology());
           // StormSubmitter.submitTopology("mytopology", config, builder.createTopology());
            Thread.sleep(10000);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            cluster.shutdown();
        }
    }
}
