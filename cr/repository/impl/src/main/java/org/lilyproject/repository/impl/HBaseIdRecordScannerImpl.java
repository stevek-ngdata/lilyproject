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
package org.lilyproject.repository.impl;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdRecordScanner;
import org.lilyproject.repository.api.RepositoryException;

/**
 *
 */
public class HBaseIdRecordScannerImpl extends AbstractHBaseRecordScanner<IdRecord> implements IdRecordScanner {

    private final RecordDecoder recordDecoder;

    public HBaseIdRecordScannerImpl(ResultScanner hbaseScanner, RecordDecoder recordDecoder) {
        super(hbaseScanner);
        this.recordDecoder = recordDecoder;
    }

    @Override
    IdRecord decode(Result result) throws RepositoryException, InterruptedException {
        return this.recordDecoder.decodeRecordWithIds(result);
    }

}
