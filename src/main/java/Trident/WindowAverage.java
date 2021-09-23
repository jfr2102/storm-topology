package Trident;

import com.codahale.metrics.Counter;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;

public class WindowAverage extends BaseAggregator<WindowAverage.AggregateState> {
    Counter window_count;

    static class AggregateState {
        long count = 0;
        long sum = 0;
        long max_ts = Long.MIN_VALUE;
        long min_ts = Long.MAX_VALUE;
    }

    @Override
    public AggregateState init(Object batchId, TridentCollector collector) {
        return new AggregateState();
    }

    public void aggregate(AggregateState state, TridentTuple tuple, TridentCollector collector) {
        state.count += 1;
        state.sum += tuple.getLongByField("sensordata");

        long timestamp = tuple.getLongByField("timestamp");
        if (state.max_ts < timestamp) {
            state.max_ts = timestamp;
        }
        if (state.min_ts > timestamp) {
            state.min_ts = timestamp;
        }
    }

    @Override
    public void complete(AggregateState state, TridentCollector collector) {
        window_count.inc();
        JSONObject json_message = new JSONObject();
        long window_average = state.sum / state.sum;
        json_message.put("window_avg", window_average);
        json_message.put("start_event_time", state.min_ts);
        json_message.put("end_event_time", state.max_ts);
        json_message.put("window_size", state.count);
        json_message.put("last_event_ts", state.max_ts);
        String kafkaMessage = json_message.toString();
        String kafkaKey = "window_id: " + window_count.getCount();

        collector.emit(new Values(kafkaKey, kafkaMessage));

    }

    public WindowAverage() {

    }

    public static Fields getOutPutFields() {
        return new Fields("key", "value");
    }
}
