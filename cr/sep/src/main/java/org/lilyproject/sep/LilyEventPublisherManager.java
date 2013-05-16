/*
 * Copyright 2013 NGDATA nv
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
package org.lilyproject.sep;

import java.io.IOException;
import java.util.Map;

import org.lilyproject.util.hbase.RepoAndTableUtil;

import com.google.common.collect.Maps;
import com.ngdata.sep.EventPublisher;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;

public class LilyEventPublisherManager {

    private HBaseTableFactory tableFactory;
    private Map<String,EventPublisher> eventPublishers;

    public LilyEventPublisherManager(HBaseTableFactory tableFactory) {
        this.tableFactory = tableFactory;
        eventPublishers = Maps.newHashMap();
    }

    public synchronized EventPublisher getEventPublisher(String repositoryName, String tableName)
            throws IOException, InterruptedException {
        String hbaseTableName = RepoAndTableUtil.getHBaseTableName(repositoryName, tableName);
        if (!eventPublishers.containsKey(hbaseTableName)) {
            eventPublishers.put(hbaseTableName, createEventPublisher(repositoryName, hbaseTableName));
        }
        return eventPublishers.get(hbaseTableName);
    }

    private EventPublisher createEventPublisher(String repositoryName, String tableName) throws IOException, InterruptedException {
        HTableInterface recordTable = LilyHBaseSchema.getRecordTable(tableFactory, repositoryName, tableName);
        return new LilyHBaseEventPublisher(recordTable);
    }
}
