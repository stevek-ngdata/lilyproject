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

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.util.repo.VersionTag;

@Path("record/{id}/vtag/{vtag}")
public class RecordByVtagResource extends RepositoryEnabled {

    @GET
    @Produces("application/json")
    public Entity<Record> get(@PathParam("id") String id, @PathParam("vtag") String vtag, @Context UriInfo uriInfo) {
        List<QName> fieldQNames = ResourceClassUtil.parseFieldList(uriInfo);

        Repository repository = getRepository(uriInfo);
        RecordId recordId = repository.getIdGenerator().fromString(id);
        Record record;
        try {
            record = VersionTag.getRecord(recordId, vtag, fieldQNames, repository);
            if (record == null) {
                throw new ResourceException("Undefined version tag: " + vtag, NOT_FOUND.getStatusCode());
            }
        } catch (ResourceException e) {
            // was thrown above, just throw further on, don't want to catch it here below
            throw e;
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (FieldTypeNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
        return Entity.create(record, uriInfo);

    }

}
