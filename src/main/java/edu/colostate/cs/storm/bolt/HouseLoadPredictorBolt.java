package edu.colostate.cs.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.colostate.cs.storm.Constants;
import edu.colostate.cs.storm.common.AverageTracker;
import edu.colostate.cs.storm.common.SummaryArchive;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: Thilina
 * Date: 10/31/14
 */
public class HouseLoadPredictorBolt extends BaseBasicBolt {

    private long currentSliceStart;
    private long sliceLength = 60l;
    private int tickCounter = 0;

    private Map<String, AverageTracker> trackers = new HashMap<String, AverageTracker>();
    private Map<String, SummaryArchive> archiveMap = new HashMap<String, SummaryArchive>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Constants.PredictionOutFields.TIMESTAMP,
                Constants.PredictionOutFields.HOUSE_ID,
                Constants.PredictionOutFields.PREDICTED_LOAD));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // first get the house id
        int type = tuple.getIntegerByField(Constants.InputTupleFields.PROPERTY);
        // check if it's a tick tuple
        if(type ==  Constants.TICK_TUPLE){
            tickCounter = (tickCounter + 1) % 2;
            // time to emit
            if(tickCounter == 0){
                emitOutputStream(basicOutputCollector);
            }
            return;
        }

        if(type == Constants.MEASUREMENT_WORK){
            return;
        }

        String houseId = tuple.getStringByField(Constants.InputTupleFields.HOUSE_ID);
        AverageTracker averageTracker = getTracker(houseId);
        Long timeStamp = tuple.getLongByField(Constants.InputTupleFields.TIMESTAMP);
        Double value = null;
        try {
            value = tuple.getDoubleByField(Constants.InputTupleFields.VALUE);
        } catch (RuntimeException e) {
            e.printStackTrace();
            System.out.println("Error Tuple: " + tuple);
        }
        // Initialize the current slice
        if(currentSliceStart == 0l){
            currentSliceStart = timeStamp;
        }
        // Check the slice
        // This update is within current slice.
        if((currentSliceStart + sliceLength) >= timeStamp){
            averageTracker.track(value);
        } else {    // start a new slice
            startSlice();
            currentSliceStart =  currentSliceStart + sliceLength;
            // there may be slices without any records.
            while((currentSliceStart + sliceLength) < timeStamp){
                startSlice();
                currentSliceStart = currentSliceStart + sliceLength;
            }
            averageTracker.track(value);
        }
    }

    private AverageTracker getTracker(String trackerId){
        AverageTracker tracker;
        if(trackers.containsKey(trackerId)){
            tracker = trackers.get(trackerId);
        } else {
            tracker = new AverageTracker();
            trackers.put(trackerId, tracker);
        }
        return  tracker;
    }

    private SummaryArchive getSummaryArchive(String trackerId){
        SummaryArchive archive;
        if(archiveMap.containsKey(trackerId)){
            archive = archiveMap.get(trackerId);
        } else {
            archive = new SummaryArchive(sliceLength);
            archiveMap.put(trackerId, archive);
        }
        return archive;
    }

    private void emitOutputStream(BasicOutputCollector outputCollector){
        for(String houseId: trackers.keySet()){
            double currentAvg = trackers.get(houseId).retrieve();
            double median = 0;
            if(archiveMap.containsKey(houseId)){
                median = archiveMap.get(houseId).getMedian();
            }
            double prediction = predict(currentAvg, median);
            long predictedTimeStamp = currentSliceStart + 2 * sliceLength;
            outputCollector.emit(new Values(predictedTimeStamp, houseId, prediction));
        }
    }

    private double predict(double currentAvg, double median){
        return currentAvg + median;
    }

    private void startSlice(){
        for(String houseId: trackers.keySet()){
            AverageTracker tracker = getTracker(houseId);
            getSummaryArchive(houseId).archive(tracker.retrieve());
            tracker.reset();
        }

    }

}
