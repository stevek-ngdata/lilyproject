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

import static com.google.common.collect.Iterables.toArray;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.runtime.rapi.ConfRegistry;
import org.lilyproject.servletregistry.api.ServletFilterRegistryEntry;
import org.lilyproject.servletregistry.api.ServletRegistry;
import org.lilyproject.servletregistry.api.ServletRegistryEntry;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

public class CustomJettyLauncher {

    public static final String LILY_SSL_KEYSTORE = "lily.ssl.keystore";

    private Log log = LogFactory.getLog(CustomJettyLauncher.class);

    @Autowired
    private ServletRegistry servletRegistry;

    @Autowired
    private ConfRegistry confRegistry;

    private Server server;
    private Context context;

    public CustomJettyLauncher() throws Exception {
        log.trace("in constructor custom jetty launcher");
    }

    @PostConstruct
    public Server getServerInstance() throws Exception {
        if (server != null) return server;

        log.trace("starting jetty http server.");
        server = new Server();
        ArrayList<Connector> connectors = Lists.newArrayList();
        addSSLConnector(connectors);
        addPlainConnector(connectors);

        server.setConnectors(toArray(connectors, Connector.class));
        if (sessions()) {
            context = new Context(server, "/", Context.SESSIONS);
        } else {
            context = new Context(server, "/", Context.NO_SESSIONS);
        }

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

    private void addPlainConnector(List<Connector> connectors) {
        SelectChannelConnector selectChannelConnector = new SelectChannelConnector();
        selectChannelConnector.setPort(httpPort());
        if (startSSL())
            selectChannelConnector.setConfidentialPort(httpsPort());
        connectors.add(selectChannelConnector);
    }

    private void addSSLConnector(List<Connector> connectors) {
        if (!startSSL()) return;

        SslSocketConnector sslConnector = new SslSocketConnector();
        sslConnector.setPort(httpsPort());

        if (StringUtils.hasText(keystore())){
            sslConnector.setKeystore(keystore());
            sslConnector.setTruststore(truststore());
        }

        sslConnector.setPassword(keystorePassword());
        sslConnector.setKeyPassword(keyPassword());
        sslConnector.setTrustPassword(truststorePassword());

        connectors.add(sslConnector);
    }

    private int httpsPort() {
        return confRegistry.getConfiguration("jetty").getChild("httpsPort").getValueAsInteger(12443);
    }

    private int httpPort() {
        return confRegistry.getConfiguration("jetty").getChild("httpPort").getValueAsInteger(12060);
    }

    private boolean startSSL() {
        return confRegistry.getConfiguration("jetty").getChild("ssl").getChild("startSSL").getValueAsBoolean(false);
    }

    private String keystore() {
        String keystore = confRegistry.getConfiguration("jetty").getChild("ssl").getChild("keystore").getValue(null);
        if (keystore == null){
            keystore = System.getProperty(LILY_SSL_KEYSTORE);
        }
        return keystore;
    }

    private String truststore() {
        return confRegistry.getConfiguration("jetty").getChild("ssl").getChild("truststore").getValue(keystore());
    }

    private String keystorePassword() {
        return confRegistry.getConfiguration("jetty").getChild("ssl").getChild("keystorePassword").getValue("changeit");
    }

    private String truststorePassword() {
        return confRegistry.getConfiguration("jetty").getChild("ssl").getChild("truststorePassword").getValue(keystorePassword());
    }

    private String keyPassword() {
        return confRegistry.getConfiguration("jetty").getChild("ssl").getChild("keyPassword").getValue(keystorePassword());
    }

    private boolean sessions(){
        return confRegistry.getConfiguration("jetty").getChild("sessions").getValueAsBoolean(true);
    }
}
