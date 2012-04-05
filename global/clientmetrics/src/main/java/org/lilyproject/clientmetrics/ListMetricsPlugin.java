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
package org.lilyproject.clientmetrics;

import java.util.ArrayList;
import java.util.List;

public class ListMetricsPlugin implements MetricsPlugin {
    private List<MetricsPlugin> plugins = new ArrayList<MetricsPlugin>();

    public void add(MetricsPlugin plugin) {
        this.plugins.add(plugin);
    }

    @Override
    public void beforeReport(Metrics metrics) {
        for (MetricsPlugin plugin : plugins) {
            plugin.beforeReport(metrics);
        }
    }

    @Override
    public void afterIncrement(Metrics metrics) {
        for (MetricsPlugin plugin : plugins) {
            plugin.afterIncrement(metrics);
        }
    }

    @Override
    public List<String> getExtraInfoLines() {
        List<String> result = new ArrayList<String>();
        for (MetricsPlugin plugin : plugins) {
            result.addAll(plugin.getExtraInfoLines());
        }
        return result;
    }
}
