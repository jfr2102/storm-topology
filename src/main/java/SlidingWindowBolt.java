import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.*;
import org.apache.storm.windowing.TupleWindow;

import org.json.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import com.codahale.metrics.Counter;

public class SlidingWindowBolt extends BaseWindowedBolt {
    private OutputCollector collector;
    private Counter counter;
    private Counter windowCounter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counter = context.registerCounter("WindowBolt_Executed");
        this.windowCounter = context.registerCounter("WindowBolt_WindowNumber");
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        inputWindow.getExpired();
        long window_sum = 0;
        long window_length = 0;
        long ts = 0;
        long max_ts = 0;
        long start_event_time = inputWindow.getStartTimestamp();
        long end_event_time = inputWindow.getEndTimestamp();

        Map<String, AvgState> map = new HashMap<String, AvgState>();

        for (Tuple tuple : inputWindow.get()) {
            Long sensordata = tuple.getLongByField("sensordata");
            window_sum += sensordata;
            ts = tuple.getLongByField("timestamp");

            if (ts > max_ts) {
                max_ts = ts;
            } else {
                //
            }
            String city = tuple.getStringByField("city");
            AvgState state = map.get(city);
            if (state == null){
                state = new AvgState(sensordata,1);
            }
            map.put(city, new AvgState(state.sum+sensordata, state.count + 1));
            counter.inc();
            window_length++;
        }

        long window_avg = window_sum / window_length;

        // mit the results
        JSONObject json_message = new JSONObject();
        json_message.put("window_avg", window_avg);
        json_message.put("start_event_time", start_event_time);
        json_message.put("end_event_time", end_event_time);
        json_message.put("window_size", window_length);
        json_message.put("last_event_ts", max_ts);
        json_message.put("count_per_city", print(map));
        String kafkaMessage = json_message.toString();
        String kafkaKey = "window_id: " + windowCounter.getCount();

        collector.emit(new Values(kafkaKey, kafkaMessage));
        windowCounter.inc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "message"));

    }
    public String print(Map<String, AvgState> map) {
        StringBuilder mapAsString = new StringBuilder("{");
        for (String key : map.keySet()) {
            AvgState state = map.get(key);
            mapAsString.append(key + "=" + state.count + ", ");
        }
        mapAsString.delete(mapAsString.length()-2, mapAsString.length()).append("}");
        return mapAsString.toString();
    }
}
class AvgState{
    public long sum;
    public long count;

    public AvgState(long sum, long count) {
        this.sum = sum;
        this.count = count;
    }
}