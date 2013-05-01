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

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.Random;

import com.google.common.cache.Cache;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RepositoryException;
import org.springframework.beans.factory.annotation.Autowired;

@Path("scan")
public class RecordScanCollectionResource extends RepositoryEnabled {
    @Autowired
    private Cache<String, RecordScanner> recordScannerMap;

    private static Random rand = new Random();

    @POST
    @Consumes("application/json")
    public Response post(RecordScan scan, @Context UriInfo uriInfo) {
        String scanId = String.valueOf(rand.nextLong());
        try {
            recordScannerMap.put(scanId, getTable(uriInfo).getScanner(scan));
        } catch (RepositoryException e) {
           throw new ResourceException(e, Status.BAD_REQUEST.getStatusCode());
        } catch (InterruptedException e) {
            throw new ResourceException(e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        URI uri = uriInfo.getBaseUriBuilder().path(RecordScanResource.class).build(scanId);
        return Response.created(uri).build();
    }
}
