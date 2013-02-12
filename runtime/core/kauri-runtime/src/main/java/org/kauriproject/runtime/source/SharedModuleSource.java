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
package org.kauriproject.runtime.source;

import org.kauriproject.runtime.rapi.ModuleSource;
import org.kauriproject.runtime.rapi.Mode;
import org.apache.commons.jci.monitor.FilesystemAlterationListener;

import java.io.InputStream;
import java.io.IOException;
import java.io.File;
import java.util.List;

public class SharedModuleSource implements ModuleSource {
    private ModuleSource delegate;
    private ModuleSourceManager manager;
    private String key;
    private int refCount = 0;

    protected SharedModuleSource(ModuleSource delegate, ModuleSourceManager manager, String key) {
        this.delegate = delegate;
        this.manager = manager;
        this.key = key;
    }

    public Resource getResource(String path) {
        return delegate.getResource(path);
    }

    public Resource getClasspathResource(String path) {
        return delegate.getClasspathResource(path);
    }

    public InputStream getClassLoaderConfig() throws IOException {
        return delegate.getClassLoaderConfig();
    }

    public List<SpringConfigEntry> getSpringConfigs(Mode mode) {
        return delegate.getSpringConfigs(mode);
    }

    public File getClassPathEntry() {
        return delegate.getClassPathEntry();
    }

    public boolean supportsListening() {
        return delegate.supportsListening();
    }

    public boolean isSourceMode() {
        return delegate.supportsListening();
    }

    public void addResourceListener(String path, FilesystemAlterationListener listener) {
        delegate.addResourceListener(path, listener);
    }

    public void removeResourceListener(FilesystemAlterationListener listener) {
        delegate.removeResourceListener(listener);
    }

    public synchronized void dispose() throws Exception {
        refCount--;
        if (refCount == 0)
            manager.dispose(this);
    }

    protected synchronized void increaseRefCount() {
        refCount++;
    }

    protected ModuleSource getDelegate() {
        return delegate;
    }

    protected String getKey() {
        return key;
    }
}