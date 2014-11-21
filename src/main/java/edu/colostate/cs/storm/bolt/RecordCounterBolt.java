package edu.colostate.cs.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Author: Thilina
 * Date: 10/16/14
 */
public class RecordCounterBolt extends BaseBasicBolt {

    private int counter = 0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        counter++;
        if(counter % 1000 == 0){
            System.out.println("Counter: " + counter);
        }
        basicOutputCollector.emit(new Values(counter));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("count"));
    }
}
