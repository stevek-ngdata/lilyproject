package org.lilyproject.server.modules.general;

import org.lilyproject.util.LilyInfo;

public class LilyInfoImpl implements LilyInfo {
    private boolean indexerMaster;
    private boolean rowLogProcessorMQ;
    private boolean rowLogProcessorWAL;

    @Override
    public String getVersion() {
        return "TODO";
    }

    @Override
    public boolean isIndexerMaster() {
        return indexerMaster;
    }

    @Override
    public boolean isRowLogProcessorMQ() {
        return rowLogProcessorMQ;
    }

    @Override
    public boolean isRowLogProcessorWAL() {
        return rowLogProcessorWAL;
    }

    @Override
    public void setIndexerMaster(boolean indexerMaster) {
        this.indexerMaster = indexerMaster;
    }

    @Override
    public void setRowLogProcessorMQ(boolean rowLogProcessorMQ) {
        this.rowLogProcessorMQ = rowLogProcessorMQ;
    }

    @Override
    public void setRowLogProcessorWAL(boolean rowLogProcessorWAL) {
        this.rowLogProcessorWAL = rowLogProcessorWAL;
    }
}
