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
package org.lilyproject.tenant.model.api;

public class TenantModelEvent {
    private TenantModelEventType eventType;
    private String tenantName;

    public TenantModelEvent(TenantModelEventType eventType, String tenantName) {
        this.eventType = eventType;
        this.tenantName = tenantName;
    }

    public TenantModelEventType getEventType() {
        return eventType;
    }

    public String getTenantName() {
        return tenantName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TenantModelEvent that = (TenantModelEvent)o;

        if (eventType != that.eventType) return false;
        if (!tenantName.equals(that.tenantName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = eventType.hashCode();
        result = 31 * result + tenantName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TenantModelEvent{" +
                "eventType=" + eventType +
                ", tenantName='" + tenantName + '\'' +
                '}';
    }
}
