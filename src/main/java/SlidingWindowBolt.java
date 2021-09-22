import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.*;
import org.apache.storm.windowing.TupleWindow;

import org.json.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
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
        long window_sum = 0;
        long window_length = 0;
        long ts = 0;
        long max_ts = 0;
        long start_event_time = inputWindow.getStartTimestamp();
        long end_event_time = inputWindow.getEndTimestamp();

        for (Tuple tuple : inputWindow.get()) {
            window_sum += tuple.getLongByField("sensordata");
            // was wenn innerhalb window tuple failed eig. müsste ganzes window failed
            // werden? -> stateful window later
            ts = tuple.getLongByField("timestamp");

            if (ts > max_ts) {
                max_ts = ts;
            } else {
                //
            }

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
        String kafkaMessage = json_message.toString();
        String kafkaKey = "window_id: " + windowCounter.getCount();

        // String kafkaMessage = "{ window_avg: " + window_avg + ", start_event_time: "
        // + start_event_time
        // + ", end_event_time: " + end_event_time + " window_size: " + window_length +
        // " }";

        collector.emit(new Values(kafkaKey, kafkaMessage));
        windowCounter.inc();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "message"));

    }

}