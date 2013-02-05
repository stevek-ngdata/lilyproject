package org.lilyproject.servletregistry.api;

import javax.servlet.Servlet;
import java.util.EventListener;
import java.util.List;

public interface ServletRegistryEntry {
  
  String getUrlMapping();
  Servlet getServletInstance();
  List<EventListener> getEventListeners();

}
