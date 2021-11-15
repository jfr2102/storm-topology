import com.codahale.metrics.Counter;
import org.apache.storm.task.*;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class LateTupleBolt extends BaseRichBolt {
    OutputCollector collector;
    Counter late_tuples;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        late_tuples = context.registerCounter("late_tuple_Bolts");
    }

    @Override
    public void execute(Tuple input) {
    late_tuples.inc();
    collector.ack(input);
    }
}
