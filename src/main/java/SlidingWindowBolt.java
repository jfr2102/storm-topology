import org.apache.storm.topology.base.*;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
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
        for (Tuple tuple : inputWindow.get()) {
            window_sum = window_sum + tuple.getLongByField("temp");
            collector.ack(tuple);
            counter.inc();
        }
        // emit the results
        collector.emit(new Values(window_sum));
        windowCounter.inc();
    }

}