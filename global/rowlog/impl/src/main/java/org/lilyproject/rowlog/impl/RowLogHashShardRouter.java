package org.lilyproject.rowlog.impl;

import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogShard;
import org.lilyproject.rowlog.api.RowLogShardList;
import org.lilyproject.rowlog.api.RowLogShardRouter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class RowLogHashShardRouter implements RowLogShardRouter {
    private final MessageDigest mdAlgorithm;

    public RowLogHashShardRouter() {
        try {
            mdAlgorithm = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public RowLogShard getShard(RowLogMessage message, RowLogShardList shardList) {
        List<RowLogShard> shards = shardList.getShards();
        if (shards.isEmpty()) {
            throw new IllegalStateException("There are not rowlog shards registered.");
        }
        long hash = hash(message.getRowKey());
        int selectedShard = (int)(hash % shards.size());
        return shards.get(selectedShard);
    }

    private long hash(byte[] rowKey) {
        try {
            // Cloning message digest rather than looking it up each time
            MessageDigest md = (MessageDigest)mdAlgorithm.clone();
            byte[] digest = md.digest(rowKey);
            return ((digest[0] & 0xFF) << 8) + ((digest[1] & 0xFF));
        } catch (CloneNotSupportedException e) {
            // Sun's MD5 supports cloning, so we don't expect this to happen
            throw new RuntimeException(e);
        }
    }

}
