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
        public static final String PLUG_SPECIFIC_KEY = "plug-specific-key";
        public static final String SLIDING_WINDOW_START = "sliding-window-start";
        public static final String SLIDING_WINDOW_END = "sliding-window-end";
        public static final String OUTLIER_PERCENTAGE = "outlier-percentage";
        public static final String PER_PLUG_MEDIAN = "per-plug-median";
        public static final String TUPLE_COUNT = "tuple-count";
    }

    public static final int MEASUREMENT_WORK = 0;
    public static final int MEASUREMENT_LOAD = 1;

    public static final String SLICE_LENGTH = "slice-length";
    public static final int SLIDING_WINDOW_ADD = 1;
    public static final int SLIDING_WINDOW_REMOVE = -1;
    public static final String S3_BUCKET_NAME = "s3-bucket";
    public static final String S3_KEY = "s3-key";
    public static final String S3_OUTPUT_KEY = "s3-output";
    public static final String MODE = "mode";
    public static final String MODE_REMOTE = "remote";

    public class Streams {
        public static final String POWER_GRID_DATA = "power-grid-data";
        public static final String CUSTOM_TICK_TUPLE = "custom-tick-stream";
        public static final String SLIDING_WINDOW_STREAM = "sliding-window-stream";
        public static final String GLOBAL_MEDIAN_STREAM = "global-median-stream";
        public static final String PER_PLUG_MEDIAN_STREAM = "per-plug-median-stream";
        public static final String OUTLIER_STREAM = "outlier-stream";
        public static final String PERF_PUNCTUATION_STREAM = "perf-punctuation-stream";
        public static final String HOUSE_LOAD_PREDICTION = "house-load-prediction";
        public static final String PLUG_LOAD_PREDICTION = "plug-load-prediction";
    }
}
