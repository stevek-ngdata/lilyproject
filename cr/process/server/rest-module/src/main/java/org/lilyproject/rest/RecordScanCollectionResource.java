package org.lilyproject.rest;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.RecordScanner;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RepositoryException;

@Path("scan")
public class RecordScanCollectionResource extends RepositoryEnabled {
    private static Map<String,RecordScanner> scanMap = Collections.synchronizedMap(new HashMap<String, RecordScanner>()); 
    private static Random rand = new Random();
    
    @POST
    @Consumes("application/json")
    public Response post(RecordScan scan) {
        String scanId = String.valueOf(rand.nextLong());        
        try {
            scanMap.put(scanId, repository.getScanner(scan));
        } catch (RepositoryException e) {
           throw new ResourceException(e, Status.BAD_REQUEST.getStatusCode());           
        } catch (InterruptedException e) {
            throw new ResourceException(e, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        URI uri = UriBuilder.fromResource(RecordScanResource.class).build(scanId);
        return Response.created(uri).build();     
    }
    
    protected static boolean hasScanner(String scannerId) {
        return RecordScanCollectionResource.scanMap.containsKey(scannerId);
    }
    protected static RecordScanner getScanner(String scannerId) {
        return RecordScanCollectionResource.scanMap.get(scannerId);
    }
    protected static RecordScanner removeScanner(String scannerId) {
        return RecordScanCollectionResource.scanMap.remove(scannerId);
    }
}
