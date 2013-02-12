package org.lilyproject.servlet;

import javax.servlet.ServletException;
import java.util.Map;

import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.servlet.WebConfig;
import com.sun.jersey.spi.spring.container.servlet.SpringServlet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ConfigurableApplicationContext;

public class JerseySpringServlet extends SpringServlet {

    private static final Log log = LogFactory.getLog(JerseySpringServlet.class);

    private ConfigurableApplicationContext context;

    public JerseySpringServlet(ConfigurableApplicationContext context) {
        this.context = context;
    }
    @Override
    protected ResourceConfig getDefaultResourceConfig(Map<String, Object> props,
                                                      WebConfig webConfig) throws ServletException {
//        props.put(PackagesResourceConfig.PROPERTY_PACKAGES, "com.ngdata.lilyenterprise.consumerdb.rest");
//        return new PackagesResourceConfig(props);
        return new DefaultResourceConfig();
    }


    @Override
    protected ConfigurableApplicationContext getContext() {
        return context;
    }
}