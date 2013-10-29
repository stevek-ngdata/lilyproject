package org.lilyproject.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.io.InputStream;


@Path("version")
public class VersionResource {

    @GET
    @Produces("application/json")
    public String get(@Context UriInfo uriInfo) {
        Properties versionProperties = new Properties();

        try {
            ClassLoader cl = this.getClass().getClassLoader();
            InputStream versionConfig = cl.getResourceAsStream("version.properties");
            versionProperties.load(versionConfig);
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


        return "Version: " + versionProperties.getProperty("version") + "\nBuild Date: " + versionProperties.get("buildtime");
    }

}
