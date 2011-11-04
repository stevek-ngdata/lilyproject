package org.lilyproject.rowlog.api;

public interface RowLogShardRouter {
    RowLogShard getShard(RowLogMessage message, RowLogShardList shardList) throws RowLogException;
}
