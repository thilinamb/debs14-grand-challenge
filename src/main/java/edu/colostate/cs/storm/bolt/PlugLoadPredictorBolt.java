package edu.colostate.cs.storm.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.colostate.cs.storm.Constants;

/**
 * Author: Thilina
 * Date: 11/22/14
 */
public class PlugLoadPredictorBolt extends LoadPredictorBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Constants.DataFields.TIMESTAMP,
                Constants.DataFields.HOUSE_ID,
                Constants.DataFields.HOUSEHOLD_ID,
                Constants.DataFields.PLUG_ID,
                Constants.DataFields.PREDICTED_LOAD));
    }

    @Override
    protected String getKey(Tuple tuple) {
        return tuple.getStringByField(Constants.DataFields.HOUSE_ID) + ":" +
                tuple.getStringByField(Constants.DataFields.HOUSEHOLD_ID) + ":" +
                tuple.getStringByField(Constants.DataFields.PLUG_ID);
    }

    @Override
    protected Values getOutputTuple(long predictedTimeStamp, String keyString, double predictedValue) {
        String[] segments = keyString.split(":");
        return new Values(predictedTimeStamp, segments[0], segments[1], segments[2], predictedValue);
    }
}
