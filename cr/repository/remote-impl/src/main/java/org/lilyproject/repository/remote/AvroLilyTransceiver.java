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
package org.lilyproject.repository.remote;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.avro.ipc.NettyTransceiver;

import org.apache.avro.ipc.specific.SpecificRequestor;
import org.lilyproject.avro.AvroLily;
import org.lilyproject.avro.NettyTransceiverFactory;

/**
 * Encapsulates combined creation of Transceiver and AvroLily into a single
 * object for simplified construction and testing of Avro repository IPC.
 */
public class AvroLilyTransceiver {

    private NettyTransceiver transceiver;
    private AvroLily lilyProxy;

    public AvroLilyTransceiver(InetSocketAddress address) throws IOException {
        transceiver = NettyTransceiverFactory.create(address);
        lilyProxy = SpecificRequestor.getClient(AvroLily.class, transceiver);
    }

    public NettyTransceiver getTransceiver() {
        return transceiver;
    }

    public AvroLily getLilyProxy() {
        return lilyProxy;
    }

}
