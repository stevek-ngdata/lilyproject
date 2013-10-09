package org.lilyproject.servlet.jersey;

import javax.annotation.PostConstruct;
import javax.servlet.ServletRequestListener;

import org.lilyproject.servletregistry.api.ServletRegistry;
import org.lilyproject.servletregistry.api.ServletRequestListenerRegistryEntry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.request.RequestContextListener;

public class SpringRequestContextListenerManager {

    @Autowired
    private ServletRegistry servletRegistry;

    @PostConstruct
    public void createAndRegisterInContainer() {
        servletRegistry.addServletRequestListenerEntry(new SpringRequestContextListenerRegistryEntry());
    }

    public static class SpringRequestContextListenerRegistryEntry implements ServletRequestListenerRegistryEntry {
        @Override
        public ServletRequestListener getListenerInstance() {
            return new RequestContextListener();
        }
    }
}
