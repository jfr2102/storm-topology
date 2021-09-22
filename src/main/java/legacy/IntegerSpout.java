package legacy;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
//import java.util.UUID;

import java.util.Map;

public class IntegerSpout extends BaseRichSpout {
    private int temp;
    private static String place = "München";
    private SpoutOutputCollector spoutOutputCollector;
    private long msgId = 0;

    public void open(Map<String, Object> map, TopologyContext topologyContext,
            SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    public void nextTuple() {
        spoutOutputCollector.emit(new Values(temp, ((temp % 2 == 0) ? place : "Bamberg"), System.nanoTime()), ++msgId);
        if (temp >= 100000) {
            temp = 0;
        } else {
            temp++;
        }
        // Utils.sleep(1);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("temp", "place", "time"));
    }
}
