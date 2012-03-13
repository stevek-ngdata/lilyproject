package org.lilyproject.rest;

import java.net.URI;
import java.util.Random;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RepositoryException;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.cache.Cache;

@Path("scan")
public class RecordScanCollectionResource extends RepositoryEnabled {
    @Autowired
    private Cache<String, RecordScanner> recordScannerMap;
    
    private static Random rand = new Random();
    
    @POST
    @Consumes("application/json")
    public Response post(RecordScan scan) {
        String scanId = String.valueOf(rand.nextLong());        
        try {
            recordScannerMap.put(scanId, repository.getScanner(scan));            
        } catch (RepositoryException e) {
           throw new ResourceException(e, Status.BAD_REQUEST.getStatusCode());           
        } catch (InterruptedException e) {
            throw new ResourceException(e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        URI uri = UriBuilder.fromResource(RecordScanResource.class).build(scanId);
        return Response.created(uri).build();     
    }
}
