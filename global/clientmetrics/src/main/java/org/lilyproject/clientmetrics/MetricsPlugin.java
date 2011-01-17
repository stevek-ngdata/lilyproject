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
