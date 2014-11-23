package edu.colostate.cs.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import edu.colostate.cs.storm.Constants;

/**
 * Author: Thilina
 * Date: 11/21/14
 */
public class ReportBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if(tuple.getDoubleByField(Constants.DataFields.CURRENT_GLOBAL_MEDIAN_LOAD) > 0.0){
            System.out.println(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}
