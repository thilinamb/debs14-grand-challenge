package edu.colostate.cs.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.colostate.cs.storm.Constants;
import edu.colostate.cs.storm.util.RunningMedianCalculator;

import java.util.Map;

/**
 * Author: Thilina
 * Date: 11/23/14
 */
public class GlobalMedianCalculatorBolt extends BaseBasicBolt {

    private RunningMedianCalculator medianCalc;
    private long lastUpdatedTs;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        int operation = tuple.getIntegerByField(Constants.DataFields.SLIDING_WINDOW_ACTION);
        double value = tuple.getDoubleByField(Constants.DataFields.VALUE);
        long ts = tuple.getLongByField(Constants.DataFields.TIMESTAMP);
        if(operation == Constants.SLIDING_WINDOW_ADD){
            double median = medianCalc.getMedian(value);
            if(lastUpdatedTs < ts) {
                // the sliding window has moved
                lastUpdatedTs = ts;
                outputCollector.emit(Constants.Streams.GLOBAL_MEDIAN_STREAM, new Values(ts, median));
            }
        } else {
            medianCalc.remove(value);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.Streams.GLOBAL_MEDIAN_STREAM,
                new Fields(Constants.DataFields.TIMESTAMP, Constants.DataFields.CURRENT_GLOBAL_MEDIAN_LOAD));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        medianCalc = new RunningMedianCalculator();
    }
}
