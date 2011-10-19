package org.lilyproject.rowlog.impl;

import org.lilyproject.rowlog.api.RowLogMessage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class RowLogShardRouter {
    private int shardCount;
    private final MessageDigest mdAlgorithm;

    public RowLogShardRouter(int shardCount) {
        this.shardCount = shardCount;

        try {
            mdAlgorithm = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public int getShard(RowLogMessage message) {
        long hash = hash(message.getRowKey());
        int selectedShard = (int)(hash % shardCount);
        return selectedShard;
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
