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

import java.util.List;

public interface MetricsPlugin {
    /**
     * Allows to add extra Metrics add the end of each interval, before the interval-report is outputted.
     */
    void beforeReport(Metrics metrics);

    /**
     * Called once after each metric increment, hence usually very often, thus should be very lightweight.
     */
    void afterIncrement(Metrics metrics);

    /**
     * Extra heading lines to be included in the header of each interval.
     */
    List<String> getExtraInfoLines();
}
