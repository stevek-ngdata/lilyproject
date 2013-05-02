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
package org.lilyproject.rest;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.util.List;

import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.tools.restresourcegenerator.GenerateTableResource;
import org.lilyproject.tools.restresourcegenerator.GenerateTenantAndTableResource;
import org.lilyproject.tools.restresourcegenerator.GenerateTenantResource;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("record/{id}/version")
@GenerateTableResource
@GenerateTenantResource
@GenerateTenantAndTableResource
public class RecordVersionCollectionResource extends RepositoryEnabled {

    @GET
    @Produces("application/json")
    public EntityList<Record> get(@PathParam("id") String id,
            @DefaultValue("1") @QueryParam("start-index") Long startIndex,
            @DefaultValue("10") @QueryParam("max-results") Long maxResults,
            @Context UriInfo uriInfo) {

        LRepository repository = getRepository(uriInfo);
        LTable table = getTable(uriInfo);
        List<QName> fieldQNames = ResourceClassUtil.parseFieldList(uriInfo);

        RecordId recordId = repository.getIdGenerator().fromString(id);
        List<Record> records;
        try {
            records = table.readVersions(recordId, startIndex, startIndex + maxResults - 1, fieldQNames);
            return EntityList.create(records, uriInfo);
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error loading record versions.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
}
