import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;
public class KafkaBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String jsonString = input.getValue(0).toString();
        JSONObject jsonObject = new JSONObject(jsonString);
        JSONObject venue = jsonObject.getJSONObject("venue");
        String country = venue.getString("country");
        String city = venue.getString("city");
        collector.emit(new Values(country, city));
        System.out.println("{ country: " + country +", city: " + city + " }");    
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("country"));

    }

}
