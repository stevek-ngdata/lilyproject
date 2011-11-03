package org.lilyproject.rowlog.api;

import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.api.RowLogShardList;

public interface RowLogShardRouter {
    RowLogShard getShard(RowLogMessage message, RowLogShardList shardList);
}
