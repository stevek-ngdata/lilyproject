package org.lilyproject.rest;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RepositoryException;

@Path("scan/{id}")
public class RecordScanResource extends RepositoryEnabled {
    
    @GET
    @Produces("application/json")
    public EntityList<Record> get(@PathParam("id") String scanId, @DefaultValue("1") @QueryParam("batch") Long batch, @Context UriInfo uriInfo) {
        if (RecordScanCollectionResource.hasScanner(scanId)) {           
            List<Record> records = new ArrayList<Record>();
            RecordScanner scanner = RecordScanCollectionResource.getScanner(scanId);
            
            try {                
                Record record;            
                while(records.size() < batch && (record = scanner.next()) != null) {
                    records.add(record);
                }
            } catch (RepositoryException e) {
                throw new ResourceException(e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
            } catch (InterruptedException e) {
                throw new ResourceException(e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
            }
            
            if (records.size() < 1) {
                throw new ResourceException("No more records found in scanner " + scanId, Status.NO_CONTENT.getStatusCode());
            }
            
            return EntityList.create(records, uriInfo);
        } else {
            throw new ResourceException("No scan with ID " + scanId + " found", Status.NOT_FOUND.getStatusCode());
        }
    }
    
    @DELETE
    public Response delete(@PathParam("id") String scanId) {
        if (RecordScanCollectionResource.hasScanner(scanId)) { 
            RecordScanner scanner = RecordScanCollectionResource.removeScanner(scanId);
            scanner.close();     
            return Response.ok().build();
        } else {
            throw new ResourceException("No scan with ID " + scanId + " found", Status.NOT_FOUND.getStatusCode());        
        }
    }

}
