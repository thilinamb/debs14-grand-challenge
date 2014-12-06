package edu.colostate.cs.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.colostate.cs.storm.Constants;
import edu.colostate.cs.storm.util.SlidingWindow;
import edu.colostate.cs.storm.util.SlidingWindowCallback;
import edu.colostate.cs.storm.util.SlidingWindowEntry;

import java.util.List;
import java.util.Map;

/**
 * Author: Thilina
 * Date: 11/22/14
 */
public class SlidingWindowBolt extends BaseBasicBolt {

    private class SlidingWindowEntryImpl implements SlidingWindowEntry {
        private String houseId;
        private String houseHoldId;
        private String plugId;
        private long ts;
        private double value;

        private SlidingWindowEntryImpl(long ts, double value, String houseId, String houseHoldId, String plugId) {
            this.ts = ts;
            this.value = value;
            this.houseId = houseId;
            this.houseHoldId = houseHoldId;
            this.plugId = plugId;
        }

        @Override
        public long getTime() {
            return ts;
        }
    }

    private SlidingWindow window;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        window = new SlidingWindow(1 * 60 * 60);
    }

    @Override
    public void execute(Tuple tuple, final BasicOutputCollector outputCollector) {
        int type = tuple.getIntegerByField(Constants.DataFields.PROPERTY);
        // we are interested only in load
        if (type == Constants.MEASUREMENT_WORK) {
            return;
        }
        SlidingWindowEntryImpl windowEntry = new SlidingWindowEntryImpl(
                tuple.getLongByField(Constants.DataFields.TIMESTAMP),
                tuple.getDoubleByField(Constants.DataFields.VALUE),
                tuple.getStringByField(Constants.DataFields.HOUSE_ID),
                tuple.getStringByField(Constants.DataFields.HOUSEHOLD_ID),
                tuple.getStringByField(Constants.DataFields.PLUG_ID));
        window.add(windowEntry, new SlidingWindowCallback() {
            @Override
            public void remove(List<SlidingWindowEntry> entries) {
                for (SlidingWindowEntry e : entries) {
                    SlidingWindowEntryImpl entry = (SlidingWindowEntryImpl) e;
                    outputCollector.emit(Constants.Streams.SLIDING_WINDOW_STREAM,
                            new Values(entry.ts, entry.houseId, entry.houseHoldId,
                                    entry.plugId, entry.value, Constants.SLIDING_WINDOW_REMOVE));
                }
            }
        });
        outputCollector.emit(Constants.Streams.SLIDING_WINDOW_STREAM,
                new Values(windowEntry.ts, windowEntry.houseId, windowEntry.houseHoldId,
                        windowEntry.plugId, windowEntry.value, Constants.SLIDING_WINDOW_ADD));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.Streams.SLIDING_WINDOW_STREAM,
                new Fields(Constants.DataFields.TIMESTAMP,
                        Constants.DataFields.HOUSE_ID,
                        Constants.DataFields.HOUSEHOLD_ID,
                        Constants.DataFields.PLUG_ID,
                        Constants.DataFields.VALUE,
                        Constants.DataFields.SLIDING_WINDOW_ACTION));
    }

}
