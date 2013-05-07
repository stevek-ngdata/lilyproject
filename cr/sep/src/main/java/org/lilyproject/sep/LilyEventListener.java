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

import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.TenantUnavailableException;
import org.lilyproject.util.repo.RepoAndTableUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for SEP {@link EventListener}s that process events from Lily record tables.
 * Filters out non-record events and provides richer {@link LilySepEvent}s.
 */
public abstract class LilyEventListener implements EventListener {
    private RepositoryManager repositoryManager;
    private Log log = LogFactory.getLog(getClass());

    public LilyEventListener(RepositoryManager repositoryManager) {
        this.repositoryManager = repositoryManager;
    }

    @Override
    public final void processEvents(List<SepEvent> events) {
        List<LilySepEvent> lilyEvents = new ArrayList<LilySepEvent>(events.size());
        for (SepEvent event : events) {
            if (event.getPayload() == null) {
                // The event is either not from a Lily table or not a normal record operation.
                continue;
            }

            String[] tenantAndTable = RepoAndTableUtil.getTenantAndTable(Bytes.toString(event.getTable()));
            IdGenerator idGenerator = null;
            try {
                idGenerator = repositoryManager.getRepository(tenantAndTable[0]).getIdGenerator();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (TenantUnavailableException e) {
                log.warn(String.format("Skipping an event because tenant is not available. Tenant: %s, table: %s, row: %s",
                        tenantAndTable[0], tenantAndTable[1], Bytes.toStringBinary(event.getRow())));
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
            lilyEvents.add(new LilySepEvent(idGenerator, tenantAndTable[0], tenantAndTable[1], event.getTable(),
                    event.getRow(), event.getKeyValues(), event.getPayload()));
        }
        processLilyEvents(lilyEvents);
    }

    public abstract void processLilyEvents(List<LilySepEvent> sepEvents);
}
