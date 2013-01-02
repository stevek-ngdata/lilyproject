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

    public String description;

    /**
     * Each metric is associated with a number, which is its index in {@link Interval#datas}.
     */
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
