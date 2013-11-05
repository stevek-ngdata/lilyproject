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
package org.lilyproject.repository.fake;

import org.lilyproject.plugin.PluginUser;

/**
 * Dummy registry that really doesn't do anything. It doesn't need to since we won't be cleaning up after ourselves after
 * testing
 */
public class FakeRegistry implements org.lilyproject.plugin.PluginRegistry {
    public FakeRegistry() {
    }

    @Override
    public <T> void addPlugin(Class<T> tClass, String s, T t) {
    }

    @Override
    public <T> void removePlugin(Class<T> tClass, String s, T t) {
    }

    @Override
    public <T> void setPluginUser(Class<T> tClass, PluginUser<T> tPluginUser) {
    }

    @Override
    public <T> void unsetPluginUser(Class<T> tClass, PluginUser<T> tPluginUser) {
    }
}
