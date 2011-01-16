package org.lilyproject.clientmetrics.postproc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Contains the metrics of one test (one metric file may contain data from multiple, sequentially executed
 * sub-tests).
 */
public class Test {
    public String name;

    /** Each metric is associated with a number, which is its index in {@link Interval#datas}. */
    public Map<String, Integer> metricNames = new HashMap<String, Integer>();

    public List<Interval> intervals = new ArrayList<Interval>();

    public Test(String name) {
        this.name = name;
    }

    public void add(Interval interval) {
        intervals.add(interval);
    }

    public int getIndex(String metricName) {
        Integer index = metricNames.get(metricName);
        if (index == null) {
            index = metricNames.size();
            metricNames.put(metricName, index);
        }
        return index;
    }
}
