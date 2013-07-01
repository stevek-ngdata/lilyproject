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
package org.lilyproject.container.jetty;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.servletregistry.api.ServletFilterRegistryEntry;
import org.lilyproject.servletregistry.api.ServletRegistry;
import org.lilyproject.servletregistry.api.ServletRegistryEntry;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;
import org.springframework.beans.factory.annotation.Autowired;

public class CustomJettyLauncher {

    private Log log = LogFactory.getLog(CustomJettyLauncher.class);

    @Autowired
    private ServletRegistry servletRegistry;

    private Server server;
    private Context context;

    public CustomJettyLauncher() throws Exception {
        log.trace("in constructor custom jetty launcher");

        server = new Server(12060);

        context = new Context(server, "/", Context.SESSIONS);
    }

    @PostConstruct
    public Server getServerInstance() throws Exception {
        for (ServletRegistryEntry entry: servletRegistry.getEntries()) {
            ServletHolder servletHolder = new ServletHolder(entry.getServletInstance(context.getServletContext()));
            for (String pattern: entry.getUrlPatterns()) {
                context.addServlet(servletHolder, pattern);
            }
        }
        for (ServletFilterRegistryEntry servletFilterEntry : servletRegistry.getFilterEntries()) {
            FilterHolder filterHolder =
                    new FilterHolder(servletFilterEntry.getServletFilterInstance(context.getServletContext()));
            for (Map.Entry<String, Integer> patternEntry : servletFilterEntry.getUrlPatterns().entrySet()) {
                int dispatch = patternEntry.getValue() != null ? patternEntry.getValue() : Handler.DEFAULT;
                context.addFilter(filterHolder, patternEntry.getKey(), dispatch);
            }
        }
        server.start();
        return server;
    }

    @PreDestroy
    public void stopServer() throws Exception {
        server.stop();
    }

}
