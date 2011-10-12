package org.lilyproject.clientmetrics;

import java.util.Collections;
import java.util.List;

public class NullPlugin implements MetricsPlugin {
    @Override
    public void beforeReport(Metrics metrics) {
    }

    @Override
    public void afterIncrement(Metrics metrics) {
    }

    @Override
    public List<String> getExtraInfoLines() {
        return Collections.emptyList();
    }
}
