package org.lilyproject.clientmetrics;

import java.util.List;

public interface MetricsPlugin {
    /**
     * Allows to add extra Metrics add the end of each interval, before the interval-report is outputted.
     */
    void beforeReport(Metrics metrics);

    /**
     * Extra heading lines to be included in the header of each interval.
     */
    List<String> getExtraInfoLines();
}
