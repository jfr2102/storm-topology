import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        //builder.setSpout("IntegerSpout", new IntegerSpout());
        builder.setSpout("IntegerSpout", new IntegerSpout(), 3);
        builder.setBolt("MyBolt",new MyBolt(),2).shuffleGrouping("IntegerSpout");

        Config config = new Config();
        //config.setDebug(false);
        config.setNumWorkers(6);
        config.setMaxSpoutPending(50000);
        config.setNumEventLoggers(1);
        config.setStatsSampleRate(0.05);
       // bei local
        //LocalCluster cluster = new LocalCluster();

        try{
            //bei local
           // cluster.submitTopology("toplogy", config, builder.createTopology());
            //bei cluster run
            StormSubmitter.submitTopology("mytopology", config, builder.createTopology());
            Thread.sleep(1000);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
           // cluster.shutdown();
        }
    }
}
