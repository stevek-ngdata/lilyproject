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
package org.lilyproject.rowlog.impl;

import org.lilyproject.rowlog.api.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Assigns messages to shards based on the hash of the row key. Messages thus always
 * end up in the same shard (as long as the number of shards stays the same), though
 * currently this is not necessary since there is one central rowlog processor which
 * guarantees that no two messages of the same row will be processed concurrently.
 */
public class RowLogHashShardRouter implements RowLogShardRouter {
    private final MessageDigest mdAlgorithm;

    public RowLogHashShardRouter() {
        try {
            mdAlgorithm = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public RowLogShard getShard(RowLogMessage message, RowLogShardList shardList) throws RowLogException {
        List<RowLogShard> shards = shardList.getShards();
        if (shards.isEmpty()) {
            throw new RowLogException("There are no rowlog shards registered.");
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
