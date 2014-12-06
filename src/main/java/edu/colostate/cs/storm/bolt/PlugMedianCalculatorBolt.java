package edu.colostate.cs.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.colostate.cs.storm.Constants;
import edu.colostate.cs.storm.util.RunningMedianCalculator;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: Thilina
 * Date: 12/5/14
 */
public class PlugMedianCalculatorBolt extends BaseBasicBolt {

    private Map<String, RunningMedianCalculator> runningMedians = new HashMap<String, RunningMedianCalculator>();
    private Map<String, Long> lastUpdatedTsMap = new HashMap<String, Long>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        int operation = tuple.getIntegerByField(Constants.DataFields.SLIDING_WINDOW_ACTION);
        double value = tuple.getDoubleByField(Constants.DataFields.VALUE);
        long ts = tuple.getLongByField(Constants.DataFields.TIMESTAMP);
        String key = getKey(tuple);

        RunningMedianCalculator medianCalc = runningMedians.get(key);
        if(medianCalc == null){
            medianCalc =  new RunningMedianCalculator();
            runningMedians.put(key, medianCalc);
        }
        Long lastUpdatedTs = lastUpdatedTsMap.get(key);
        if(lastUpdatedTs == null){
            lastUpdatedTs = 0l;
        }

        if(operation == Constants.SLIDING_WINDOW_ADD){
            double median = medianCalc.getMedian(value);
            if(lastUpdatedTs < ts) {
                // the sliding window has moved
                lastUpdatedTsMap.put(key, ts);
                outputCollector.emit(Constants.Streams.PER_PLUG_MEDIAN_STREAM, new Values(key, ts, median));
            }
        } else {
            medianCalc.remove(value);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.Streams.PER_PLUG_MEDIAN_STREAM,
                new Fields(Constants.DataFields.PLUG_SPECIFIC_KEY, Constants.DataFields.TIMESTAMP,
                        Constants.DataFields.PER_PLUG_MEDIAN));
    }

    private String getKey(Tuple tuple){
        return tuple.getStringByField(Constants.DataFields.HOUSE_ID) + ':' +
                tuple.getStringByField(Constants.DataFields.HOUSEHOLD_ID) + ':' +
                tuple.getStringByField(Constants.DataFields.PLUG_ID);
    }

}

