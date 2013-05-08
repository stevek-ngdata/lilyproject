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
package org.lilyproject.rest;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.List;

import com.google.common.cache.Cache;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.tools.restresourcegenerator.GenerateTableResource;
import org.lilyproject.tools.restresourcegenerator.GenerateRepositoryAndTableResource;
import org.lilyproject.tools.restresourcegenerator.GenerateRepositoryResource;
import org.springframework.beans.factory.annotation.Autowired;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;

@Path("scan/{id}")
@GenerateTableResource
@GenerateRepositoryResource
@GenerateRepositoryAndTableResource
public class RecordScanResource extends BaseRepositoryResource {
    @Autowired
    private Cache<String, RecordScanner> recordScannerMap;

    @GET
    @Produces("application/json")
    public EntityList<Record> get(@PathParam("id") String scanId, @DefaultValue("1") @QueryParam("batch") Long batch, @Context UriInfo uriInfo) {
        RecordScanner scanner = recordScannerMap.getIfPresent(scanId);
        if (scanner != null) {
            List<Record> records = new ArrayList<Record>();

            try {
                Record record;
                while(records.size() < batch && (record = scanner.next()) != null) {
                    records.add(record);
                }
            } catch (RepositoryException e) {
                throw new ResourceException(e, INTERNAL_SERVER_ERROR.getStatusCode());
            } catch (InterruptedException e) {
                throw new ResourceException(e, INTERNAL_SERVER_ERROR.getStatusCode());
            }

            if (records.size() < 1) {
                throw new WebApplicationException(Response.status(NO_CONTENT).build());
            }

            return EntityList.create(records, uriInfo);
        } else {
            throw new ResourceException("No scan with ID " + scanId + " found", NOT_FOUND.getStatusCode());
        }
    }

    @DELETE
    public Response delete(@PathParam("id") String scanId) {
        RecordScanner scanner = this.recordScannerMap.getIfPresent(scanId);
        if (scanner != null) {
            scanner.close();
            this.recordScannerMap.invalidate(scanId);
            return Response.ok().build();
        } else {
            throw new ResourceException("No scan with ID " + scanId + " found", NOT_FOUND.getStatusCode());
        }
    }
}
