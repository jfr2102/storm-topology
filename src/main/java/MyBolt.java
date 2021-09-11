import com.codahale.metrics.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class MyBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private Counter counter;
    //private Timer processingLatency;

    /*
     * public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
     * String place = tuple.getValueByField("place").toString(); int temp =
     * tuple.getIntegerByField("temp"); System.out.println("place: " + place+
     * " temp: "+temp); basicOutputCollector.emit(new Values(temp+10, place)); }
     */

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("temp", "place"));
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        this.counter = topologyContext.registerCounter("MyBoltFilter_Executed");
        //this.processingLatency = topologyContext.registerTimer("Processing_Latency");
    }

    @Override
    public void execute(Tuple tuple) {
        String place = tuple.getValueByField("place").toString();
        int temp = tuple.getIntegerByField("temp");
        // simulate expensive operation?:
        /*
         * try { Thread.sleep(5); } catch (InterruptedException e) {
         * e.printStackTrace(); }
         */
        if (place.equals("Bamberg")) {
            _collector.emit(new Values(temp + 10, place));
        }
        counter.inc();
        //long latency = tuple.getLongByField("time") - System.nanoTime();
        //this.processingLatency.update(latency, java.util.concurrent.TimeUnit.NANOSECONDS);
        _collector.ack(tuple);
    }
}
