/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
