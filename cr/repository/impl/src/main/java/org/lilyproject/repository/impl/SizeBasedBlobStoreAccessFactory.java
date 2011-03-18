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
package org.lilyproject.repository.impl;

import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.BlobStoreAccessFactory;

public class SizeBasedBlobStoreAccessFactory implements BlobStoreAccessFactory {
    private Log log = LogFactory.getLog(getClass());

    private final Map<String, BlobStoreAccess> blobStoreAccesses = new HashMap<String, BlobStoreAccess>();
    private final SortedMap<Long, BlobStoreAccess> usedBlobStoreAccesses = new TreeMap<Long, BlobStoreAccess>();
    
    public SizeBasedBlobStoreAccessFactory(List<BlobStoreAccess> availableBlobStoreAccesses, BlobStoreAccessConfig blobStoreAccessConfig) {
        for (BlobStoreAccess blobStoreAccess : availableBlobStoreAccesses) {
            blobStoreAccesses.put(blobStoreAccess.getId(), blobStoreAccess);
        }
        
        BlobStoreAccess defaultBlobStoreAccess = blobStoreAccesses.get(blobStoreAccessConfig.getDefault());
        usedBlobStoreAccesses.put(Long.MAX_VALUE, defaultBlobStoreAccess);
        log.info("Setting default blobstore " + defaultBlobStoreAccess.getId());
        Map<String, Long> limits = blobStoreAccessConfig.getLimits();
        for (Entry<String, Long> limit : limits.entrySet()) {
            addBlobStoreAccess(limit.getValue(), blobStoreAccesses.get(limit.getKey()));
        }
    }

    private void addBlobStoreAccess(long upperLimit, BlobStoreAccess blobStoreAccess) {
        usedBlobStoreAccesses.put(upperLimit, blobStoreAccess);
        log.info("Setting limit "+ upperLimit+ " for blobstore " +blobStoreAccess.getId());
    }
    
    public BlobStoreAccess get(Blob blob) {
        Long size = blob.getSize();
        for (Long upperLimit: usedBlobStoreAccesses.keySet()) {
            if (size <= upperLimit) {
                 return usedBlobStoreAccesses.get(upperLimit);
            }
        }
        return usedBlobStoreAccesses.get(Long.MAX_VALUE);
    }
    
    public Collection<BlobStoreAccess> getAll() {
        return usedBlobStoreAccesses.values();
    }
}
