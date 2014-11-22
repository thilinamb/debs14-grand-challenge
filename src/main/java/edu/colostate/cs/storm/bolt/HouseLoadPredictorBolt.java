package edu.colostate.cs.storm.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.colostate.cs.storm.Constants;

/**
 * Author: Thilina
 * Date: 10/31/14
 */
public class HouseLoadPredictorBolt extends LoadPredictorBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Constants.DataFields.TIMESTAMP,
                Constants.DataFields.HOUSE_ID,
                Constants.DataFields.PREDICTED_LOAD));
    }

    @Override
    protected String getKey(Tuple tuple) {
        return tuple.getStringByField(Constants.DataFields.HOUSE_ID);
    }

    @Override
    protected Values getOutputTuple(long predictedTimeStamp, String keyString, double predictedValue) {
        return new Values(predictedTimeStamp, keyString, predictedValue);
    }
}
