package edu.colostate.cs.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.colostate.cs.storm.Constants;
import edu.colostate.cs.storm.common.AverageTracker;
import edu.colostate.cs.storm.common.SummaryArchive;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: Thilina
 * Date: 11/21/14
 */
public abstract class LoadPredictorBolt extends BaseBasicBolt {
    protected long currentSliceStart;
    protected long sliceLength = 60l;
    protected int tickCounter = 0;

    protected Map<String, AverageTracker> trackers = new HashMap<String, AverageTracker>();
    protected Map<String, SummaryArchive> archiveMap = new HashMap<String, SummaryArchive>();

    @Override
    public abstract void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);

    protected abstract String getKey(Tuple tuple);

    protected abstract Values getOutputTuple(long predictedTimeStamp, String keyString, double predictedValue);

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        sliceLength = (Long) stormConf.get(Constants.SLICE_LENGTH);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if (tuple.getSourceStreamId().equals(Constants.Streams.PERF_PUNCTUATION_STREAM)) {
            basicOutputCollector.emit(Constants.Streams.PERF_PUNCTUATION_STREAM, tuple.getValues());
            return;
        }

        if (tuple.getSourceStreamId().equals(Constants.Streams.CUSTOM_TICK_TUPLE)) {
            tickCounter = (tickCounter + 1) % 2;
            // time to emit
            if (tickCounter == 0) {
                emitOutputStream(basicOutputCollector);
            }
            return;
        }
        // first get the house id
        int type = tuple.getIntegerByField(Constants.DataFields.PROPERTY);
        // check if it's a tick tuple

        if (type == Constants.MEASUREMENT_WORK) {
            return;
        }

        AverageTracker averageTracker = getTracker(getKey(tuple));
        Long timeStamp = tuple.getLongByField(Constants.DataFields.TIMESTAMP);
        Double value = null;
        value = tuple.getDoubleByField(Constants.DataFields.VALUE);

        // Initialize the very first slice
        if (currentSliceStart == 0l) {
            currentSliceStart = timeStamp;
        }
        // Check the slice
        // This update is within current slice.
        if ((currentSliceStart + sliceLength) >= timeStamp) {
            averageTracker.track(value);
        } else {    // start a new slice
            startSlice();
            currentSliceStart = currentSliceStart + sliceLength;
            // there may be slices without any records.
            while ((currentSliceStart + sliceLength) < timeStamp) {
                startSlice();
                currentSliceStart = currentSliceStart + sliceLength;
            }
            averageTracker.track(value);
        }
    }

    private AverageTracker getTracker(String trackerId) {
        AverageTracker tracker;
        if (trackers.containsKey(trackerId)) {
            tracker = trackers.get(trackerId);
        } else {
            tracker = new AverageTracker();
            trackers.put(trackerId, tracker);
        }
        return tracker;
    }

    private SummaryArchive getSummaryArchive(String trackerId) {
        SummaryArchive archive;
        if (archiveMap.containsKey(trackerId)) {
            archive = archiveMap.get(trackerId);
        } else {
            archive = new SummaryArchive(sliceLength);
            archiveMap.put(trackerId, archive);
        }
        return archive;
    }

    protected double predict(double currentAvg, double median) {
        return currentAvg + median;
    }

    private void startSlice() {
        for (String trackerId : trackers.keySet()) {
            AverageTracker tracker = getTracker(trackerId);
            getSummaryArchive(trackerId).archive(tracker.retrieve());
            tracker.reset();
        }
    }

    protected void emitOutputStream(BasicOutputCollector outputCollector) {
        for (String key : trackers.keySet()) {
            double currentAvg = trackers.get(key).retrieve();
            double median = 0;
            if (archiveMap.containsKey(key)) {
                median = archiveMap.get(key).getMedian();
            }
            double prediction = predict(currentAvg, median);
            long predictedTimeStamp = currentSliceStart + 2 * sliceLength;
            outputCollector.emit(getStreamId(), getOutputTuple(predictedTimeStamp, key, prediction));
        }
    }

    protected abstract String getStreamId();
}
