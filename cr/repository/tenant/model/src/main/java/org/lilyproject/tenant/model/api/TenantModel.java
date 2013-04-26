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

import java.util.Set;

/**
 * Created, update and delete of {@link Tenant}s.
 */
public interface TenantModel {
    /**
     * Creates a new tenant. The tenant will initially be in the lifecycle state CREATE_REQUESTED.
     *
     * <p>Use {@link #waitUntilTenantInState} if you need to wait until a tenant is active.</p>
     */
    void create(String tenantName) throws TenantExistsException, InterruptedException, TenantModelException;

    /**
     * Soft-delete a tenant.
     *
     * <p>This doesn't delete the tenant but puts it in the lifecycle state DELETE_REQUESTED.
     * This allows cleanup operations to happen, once these are done the tenant will be
     * fully deleted.</p>
     */
    void delete(String tenantName) throws InterruptedException, TenantModelException;

    /**
     * Deletes a tenant.
     *
     * <p>Normally you shouldn't use this method but rather {@link #delete(String)}.
     * The use of this method is reserved for the TenantMaster.</p>
     */
    void deleteDirect(String tenantName) throws InterruptedException, TenantModelException, TenantNotFoundException;

    /**
     * Updates a tenant.
     */
    void updateTenant(Tenant tenant) throws InterruptedException, TenantModelException, TenantNotFoundException;

    /**
     * Gets the list of tenants.
     *
     * <p>The tenants are retrieved from the persistent storage, so this method is relatively expensive.</p>
     */
    Set<Tenant> getTenants() throws InterruptedException, TenantModelException;

    /**
     * Gets a tenants.
     *
     * <p>The tenant is retrieved from the persistent storage, so this method is relatively expensive.</p>
     */
    Tenant getTenant(String tenantName) throws InterruptedException, TenantModelException, TenantNotFoundException;

    /**
     * This method checks if a tenant exists and is active.
     *
     * <p>The implementation of this method should be fast and suited for very-frequent calling,
     * i.e. it should not perform any IO.</p>
     */
    boolean tenantExistsAndActive(String tenantName);

    /**
     * This method checks if a tenant is active, it throws an exception if the tenant does not exist.
     *
     * <p>The implementation of this method should be fast and suited for very-frequent calling,
     * i.e. it should not perform any IO.</p>
     */
    boolean tenantActive(String tenantName) throws TenantNotFoundException;

    /**
     * Waits until a tenant is in the given state. If the tenant would not yet be known, this method will wait
     * for that as well.
     *
     * <p><b>Important:</b> you need to check the return code to know if the tenant really arrived in the
     * desired state (= when true is returned).</p>
     */
    boolean waitUntilTenantInState(String tenantName, Tenant.TenantLifecycleState state, long timeout)
            throws InterruptedException;

    /**
     * Get the list of tenants and at the same time register a listener for future tenant changes.
     *
     * <p>This method assures you will get change notifications for any change compared to the
     * tenants returned by this method.</p>
     */
    Set<Tenant> getTenants(TenantModelListener listener);

    void registerListener(TenantModelListener listener);

    void unregisterListener(TenantModelListener listener);

}
