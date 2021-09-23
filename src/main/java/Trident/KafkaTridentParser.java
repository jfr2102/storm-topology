package Trident;

import org.apache.storm.trident.operation.BaseFunction;

import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;

public class KafkaTridentParser implements MapFunction {
    @Override
    public Values execute(TridentTuple input) {
        String jsonString = input.getValueByField("value").toString();
        JSONObject jsonObject = new JSONObject(jsonString);

        try {
            JSONObject venue = jsonObject.getJSONObject("venue");
            String country = venue.getString("country");
            String city = venue.getString("city");
            long sensordata = jsonObject.getLong("sensordata");
            long timestamp = input.getLongByField("timestamp");
            int partition = input.getIntegerByField("partition");
            return new Values(country, city, sensordata, timestamp, partition);

        } catch (Exception e) {
            return null;

        }
    }
    public Fields getOutputFields(){
        return new Fields("country", "city", "sensordata", "timestamp", "partition");
    }
}
