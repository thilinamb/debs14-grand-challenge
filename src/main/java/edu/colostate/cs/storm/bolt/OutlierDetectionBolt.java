package edu.colostate.cs.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.colostate.cs.storm.Constants;
import edu.colostate.cs.storm.util.FixedMap;
import edu.colostate.cs.storm.util.OutlierTracker;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Author: Thilina
 * Date: 12/6/14
 */
public class OutlierDetectionBolt extends BaseBasicBolt {

    private class ComparableTuple implements Serializable, Comparable<ComparableTuple> {

        private Tuple tuple;

        private ComparableTuple(Tuple tuple) {
            this.tuple = tuple;
        }

        @Override
        public int compareTo(ComparableTuple o) {
            return this.tuple.getLongByField(Constants.DataFields.TIMESTAMP).compareTo(
                    o.tuple.getLongByField(Constants.DataFields.TIMESTAMP));
        }
    }

    private FixedMap<Long, Double> globalMedianBacklog = new FixedMap<Long, Double>(300, 300);
    private Map<String, OutlierTracker> outliers = new HashMap<String, OutlierTracker>();
    private PriorityQueue<ComparableTuple> unprocessedMessages = new PriorityQueue<ComparableTuple>();

    @Override
    public void execute(Tuple tuple, final BasicOutputCollector outputCollector) {
        if (tuple.getSourceStreamId().equals(Constants.Streams.GLOBAL_MEDIAN_STREAM)) {
            globalMedianBacklog.put(tuple.getLongByField(Constants.DataFields.TIMESTAMP),
                    tuple.getDoubleByField(Constants.DataFields.CURRENT_GLOBAL_MEDIAN_LOAD));
            // ordered based on the timestamps
            while (!unprocessedMessages.isEmpty() &&
                    unprocessedMessages.peek().tuple.getLongByField(Constants.DataFields.TIMESTAMP).
                            equals(tuple.getLongByField(Constants.DataFields.TIMESTAMP))) {
                Tuple perPlugMedianTuple = unprocessedMessages.poll().tuple;
                processPerPlugMedianTuple(perPlugMedianTuple, outputCollector);
            }
        } else {
            processPerPlugMedianTuple(tuple, outputCollector);
        }
    }

    private void processPerPlugMedianTuple(Tuple tuple, BasicOutputCollector outputCollector) {
        String key = tuple.getStringByField(Constants.DataFields.PLUG_SPECIFIC_KEY);
        String houseId = key.split(":")[0];
        long ts = tuple.getLongByField(Constants.DataFields.TIMESTAMP);
        double val = tuple.getDoubleByField(Constants.DataFields.PER_PLUG_MEDIAN);
        if (globalMedianBacklog.containsKey(ts)) {
            OutlierTracker tracker;
            if (outliers.containsKey(houseId)) {
                tracker = outliers.get(houseId);
            } else {
                tracker = new OutlierTracker();
                outliers.put(houseId, tracker);
            }
            if (!tracker.isMember(key)) {
                tracker.addMember(key);
            }
            double globalMedian = globalMedianBacklog.get(ts);
            if (globalMedian < val) { // outlier
                if (!tracker.isOutlier(key)) {
                    tracker.addOutlier(key);
                    outputCollector.emit(Constants.Streams.OUTLIER_STREAM, new Values(ts - 24 * 60 * 60, ts,
                            houseId, tracker.getCurrentPercentage()));
                }
            } else {
                if (tracker.isOutlier(key)) {
                    tracker.removeOutlier(key);
                    //emit
                    outputCollector.emit(Constants.Streams.OUTLIER_STREAM, new Values(ts - 24 * 60 * 60, ts,
                            houseId, tracker.getCurrentPercentage()));
                }
            }
        } else {    // global median has not arrived
            unprocessedMessages.add(new ComparableTuple(tuple));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.Streams.OUTLIER_STREAM,
                new Fields(Constants.DataFields.SLIDING_WINDOW_START,
                        Constants.DataFields.SLIDING_WINDOW_END,
                        Constants.DataFields.HOUSE_ID,
                        Constants.DataFields.OUTLIER_PERCENTAGE));
    }
}
