/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.indexer;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Set;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.lilyproject.avro.AvroConverter;
import org.lilyproject.avro.AvroGenericException;
import org.lilyproject.avro.AvroIndexerException;
import org.lilyproject.avro.AvroLily;
import org.lilyproject.avro.NettyTransceiverFactory;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.util.io.Closer;

/**
 * Remote implementation of the Indexer api (via avro protocol).
 *
 *
 */
public class RemoteIndexer implements Indexer, Closeable {
    private AvroLily lilyProxy;
    private final AvroConverter converter;
    private Transceiver client;

    public RemoteIndexer(InetSocketAddress address, AvroConverter converter)
            throws IOException {

        this.converter = converter;

        this.client = NettyTransceiverFactory.create(address);
        this.lilyProxy = SpecificRequestor.getClient(AvroLily.class, client);
    }

    @Override
    public void close() throws IOException {
        Closer.close(client);
    }

    @Override
    public void index(RecordId recordId) throws IndexerException, InterruptedException {
        try {
            lilyProxy.index(converter.convert(recordId));
        } catch (AvroIndexerException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw new RuntimeException(e);
        } catch (UndeclaredThrowableException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void indexOn(RecordId recordId, Set<String> indexes) throws IndexerException, InterruptedException {
        try {
            lilyProxy.indexOn(converter.convert(recordId), new ArrayList<String>(indexes));
        } catch (AvroIndexerException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw new RuntimeException(e);
        } catch (UndeclaredThrowableException e) {
            throw new RuntimeException(e);
        }
    }
}

