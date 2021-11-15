import com.codahale.metrics.Counter;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.State;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.json.JSONObject;

import javax.xml.crypto.dsig.keyinfo.KeyValue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class StatefulWindowBolt extends BaseStatefulWindowedBolt<KeyValueState<String, AvgState>> {
    private OutputCollector collector;
    private Counter counter;
    private Counter windowCounter;
    private KeyValueState<String, AvgState> state;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counter = context.registerCounter("WindowBolt_Executed");
        this.windowCounter = context.registerCounter("WindowBolt_WindowNumber");
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        long window_sum = 0;
        long window_length = 0;
        long ts = 0;
        long max_ts = 0;
        long start_event_time = inputWindow.getStartTimestamp();
        long end_event_time = inputWindow.getEndTimestamp();
        long partition = -1;
        String note = "/";

        Map<String, AvgState> map = new HashMap<String, AvgState>();
        Iterator<Tuple> it = inputWindow.getIter();
        while (it.hasNext()) {
            Tuple tuple = it.next();
            if (window_length == 0){
                //same for whole window because of FieldsGrouping by partition
                partition = tuple.getIntegerByField("partition");
                note = tuple.getStringByField("note");
            }
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
                state = new AvgState(0,0);
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
        json_message.put("partition", partition);
        json_message.put("note", note);
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
    @Override
    public void initState(KeyValueState<String, AvgState> state) {
    this.state=state;
    }

}
