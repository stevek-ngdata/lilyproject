package org.lilyproject.repotestfw;

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
