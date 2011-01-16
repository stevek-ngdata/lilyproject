package org.lilyproject.clientmetrics.postproc;

import org.joda.time.DateTime;

public class Interval {
    private static final MetricData[] EMPTY = new MetricData[0];
    public DateTime begin;
    public MetricData[] datas = EMPTY;
    public Test owner;

    public Interval(Test owner) {
        this.owner = owner;
    }

    public void set(String metricName, MetricData data) {
        int index = owner.getIndex(metricName);
        if (index >= datas.length) {
            MetricData[] newDatas = new MetricData[index + 1];
            System.arraycopy(datas, 0, newDatas, 0, datas.length);
            datas = newDatas;
        }
        datas[index] = data;
    }
}
