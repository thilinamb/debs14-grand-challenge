package edu.colostate.cs.storm;

/**
 * Author: Thilina
 * Date: 10/31/14
 */
public class Constants {
    public class DataFields {
        public static final String ID = "id";
        public static final String TIMESTAMP = "timestamp";
        public static final String VALUE = "value";
        public static final String PROPERTY = "property";
        public static final String PLUG_ID = "plug_id";
        public static final String HOUSEHOLD_ID = "household_id";
        public static final String HOUSE_ID = "house_id";
        public static final String PREDICTED_LOAD = "predicted_load";
        public static final String CURRENT_GLOBAL_MEDIAN_LOAD = "global-median-load";
        public static final String SLIDING_WINDOW_ACTION = "sliding-window-action";
    }

    public static final int MEASUREMENT_WORK = 0;
    public static final int MEASUREMENT_LOAD = 1;
    public static final int TICK_TUPLE = 100;

    public static final String SLICE_LENGTH = "slice-length";
    public static final int SLIDING_WINDOW_ADD = 1;
    public static final int SLIDING_WINDOW_REMOVE = -1;

    public class Streams {
        public static final String POWER_GRID_DATA = "power-grid-data";
        public static final String CUSTOM_TICK_TUPLE = "custom-tick-stream";
        public static final String SLIDING_WINDOW_STREAM = "sliding-window-stream";
        public static final String GLOBAL_MEDIAN_STREAM = "global-median-stream";
    }
}
