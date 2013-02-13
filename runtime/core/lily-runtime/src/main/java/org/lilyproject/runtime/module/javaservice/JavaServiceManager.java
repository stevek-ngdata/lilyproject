/*
 * Copyright 2008 Outerthought bvba and Schaubroeck nv
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
package org.lilyproject.runtime.module.javaservice;

import org.lilyproject.runtime.KauriRTException;
import org.lilyproject.runtime.util.ObjectUtils;
import org.lilyproject.runtime.util.ArgumentValidator;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Manages the registry of Java services.
 */
public class JavaServiceManager {
    private Map<Class, List<ServiceData>> serviceRegistry = new HashMap<Class, List<ServiceData>>();

    public void addService(Class type, String moduleId, String name, Object service) {
        ArgumentValidator.notNull(type, "type");
        ArgumentValidator.notNull(moduleId, "moduleId");
        ArgumentValidator.notNull(name, "name");
        ArgumentValidator.notNull(service, "service");
        if (!type.isInterface())
            throw new ServiceConfigurationException("The provided service type should be an interface.");

        if (!type.isAssignableFrom(service.getClass()))
            throw new ServiceConfigurationException("The provided service object does not implement the interface " + type.getName());

        // By current design/usage, updates and reads will never be concurrent
        List<ServiceData> serviceDatas = serviceRegistry.get(type);
        if (serviceDatas == null) {
            serviceDatas = new ArrayList<ServiceData>();
            serviceRegistry.put(type, serviceDatas);
        }

        if (findServiceData(serviceDatas, moduleId, name) != null)
            throw new ServiceConfigurationException("Duplicate name " + name + ". There is already another exported service using this name.");

        serviceDatas.add(new ServiceData(moduleId, name, service));
    }

    public Object getService(Class type) {
        List<ServiceData> serviceDatas = getServiceDatas(type);

        if (serviceDatas.size() != 1)
            throw new AmbiguousServiceIdentificationException(type.getName());

        return serviceDatas.get(0).service;
    }

    public Object getService(Class type, String moduleId) {
        List<ServiceData> serviceDatas = getServiceDatas(type);

        serviceDatas = findServiceDatas(serviceDatas, moduleId);
        if (serviceDatas.size() == 0)
            throw new NoSuchServiceException(type.getName(), moduleId);
        if (serviceDatas.size() > 1)
            throw new AmbiguousServiceIdentificationException(type.getName(), moduleId);

        return serviceDatas.get(0).service;
    }

    public Object getService(Class type, String moduleId, String name) {
        List<ServiceData> serviceDatas = getServiceDatas(type);

        ServiceData serviceData = findServiceData(serviceDatas, moduleId, name);
        if (serviceData == null)
            throw new NoSuchServiceException(type.getName(), moduleId);

        return serviceData.service;
    }

    private List<ServiceData> getServiceDatas(Class type) {
        List<ServiceData> serviceDatas = serviceRegistry.get(type);
        if (serviceDatas == null || serviceDatas.isEmpty())
            throw new NoSuchServiceException(type.getName());
        return serviceDatas;
    }

    private List<ServiceData> findServiceDatas(List<ServiceData> serviceDatas, String moduleId) {
        List<ServiceData> result = new ArrayList<ServiceData>();
        for (ServiceData serviceData : serviceDatas) {
            if (serviceData.moduleId.equals(moduleId))
                result.add(serviceData);
        }
        return result;
    }

    private ServiceData findServiceData(List<ServiceData> serviceDatas, String moduleId, String name) {
        for (ServiceData serviceData : serviceDatas) {
            if (serviceData.moduleId.equals(moduleId) && serviceData.name.equals(name))
                return serviceData;
        }
        return null;
    }

    private static class ServiceData {
        private String moduleId;
        private String name;
        private Object service;

        public ServiceData(String moduleId, String name, Object service) {
            this.moduleId = moduleId;
            this.name = name;
            this.service = service;
        }
    }

    public void stop() {
        serviceRegistry.clear();
    }
}
