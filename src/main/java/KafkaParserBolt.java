import java.util.Map;

import com.codahale.metrics.Counter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;

public class KafkaParserBolt extends BaseRichBolt {
    private Counter counter;
    private OutputCollector collector;

    @Override
    public void execute(Tuple input) {
        String jsonString = input.getValueByField("value").toString();
        JSONObject jsonObject = new JSONObject(jsonString);

        try {
            JSONObject venue = jsonObject.getJSONObject("venue");
            String country = venue.getString("country");
            String city = venue.getString("city");
            long sensordata = jsonObject.getLong("sensordata");
            long timestamp = input.getLongByField("timestamp");
            int partition = input.getIntegerByField("partition");
            String note = jsonObject.getString("note");
            collector.ack(input);
            collector.emit(input, new Values(country, city, sensordata, timestamp, partition, note));
            counter.inc();
        } catch (Exception e) {
            System.out.println("JSON not parseable");
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("country", "city", "sensordata", "timestamp", "partition", "note"));
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        counter = context.registerCounter("Parser_Executed");
    }
}
// https://stackoverflow.com/questions/59741377/json-kafka-spout-in-apache-
// https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.4/storm-moving-data/content/kafkaspout_integration__core_storm_apis.html
// https://storm.apache.org/releases/2.1.0/storm-kafka-client.html
