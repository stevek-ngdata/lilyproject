/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.sep;

import java.util.List;

import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

/**
 * Adapts a Lily ZooKeeperItf to allow it to be used in the context of the HBase Side-Effect Processor (SEP).
 */
public class ZooKeeperItfAdapter implements com.ngdata.sep.util.zookeeper.ZooKeeperItf {

    private ZooKeeperItf wrapped;

    /**
     * Construct an adapter around a Lily ZooKeeperItf.
     * 
     * @param zk to be adapted to the HBase SEP
     */
    public ZooKeeperItfAdapter(ZooKeeperItf zk) {
        this.wrapped = zk;
    }

    @Override
    public void waitForConnection() throws InterruptedException {
        wrapped.waitForConnection();
    }

    @Override
    public void addDefaultWatcher(Watcher watcher) {
        wrapped.addDefaultWatcher(watcher);
    }

    @Override
    public void removeDefaultWatcher(Watcher watcher) {
        wrapped.removeDefaultWatcher(watcher);
    }

    @Override
    public <T> T retryOperation(com.ngdata.sep.util.zookeeper.ZooKeeperOperation<T> operation) throws InterruptedException, KeeperException {
        return wrapped.retryOperation(new ZooKeeperOperationAdapter<T>(operation));
    }

    @Override
    public boolean isCurrentThreadEventThread() {
        return wrapped.isCurrentThreadEventThread();
    }

    @Override
    public long getSessionId() {
        return wrapped.getSessionId();
    }

    @Override
    public byte[] getSessionPasswd() {
        return wrapped.getSessionPasswd();
    }

    @Override
    public int getSessionTimeout() {
        return wrapped.getSessionTimeout();
    }

    @Override
    public void addAuthInfo(String scheme, byte[] auth) {
        wrapped.addAuthInfo(scheme, auth);
    }

    @Override
    public void register(Watcher watcher) {
        wrapped.register(watcher);
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException,
            InterruptedException {
        return wrapped.create(path, data, acl, createMode);
    }

    @Override
    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, StringCallback cb, Object ctx) {
        wrapped.create(path, data, acl, createMode, cb, ctx);
    }

    @Override
    public void delete(String path, int version) throws InterruptedException, KeeperException {
        wrapped.delete(path, version);
    }

    @Override
    public void delete(String path, int version, VoidCallback cb, Object ctx) {
        wrapped.delete(path, version, cb, ctx);
    }

    @Override
    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return wrapped.exists(path, watcher);
    }

    @Override
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return wrapped.exists(path, watch);
    }

    @Override
    public void exists(String path, Watcher watcher, StatCallback cb, Object ctx) {
        wrapped.exists(path, watcher, cb, ctx);
    }

    @Override
    public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        wrapped.exists(path, watch, cb, ctx);
    }

    @Override
    public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        return wrapped.getData(path, watcher, stat);
    }

    @Override
    public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return wrapped.getData(path, watch, stat);
    }

    @Override
    public void getData(String path, Watcher watcher, DataCallback cb, Object ctx) {
        wrapped.getData(path, watcher, cb, ctx);
    }

    @Override
    public void getData(String path, boolean watch, DataCallback cb, Object ctx) {
        wrapped.getData(path, watch, cb, ctx);
    }

    @Override
    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return wrapped.setData(path, data, version);
    }

    @Override
    public void setData(String path, byte[] data, int version, StatCallback cb, Object ctx) {
        wrapped.setData(path, data, version, cb, ctx);
    }

    @Override
    public List<ACL> getACL(String path, Stat stat) throws KeeperException, InterruptedException {
        return wrapped.getACL(path, stat);
    }

    @Override
    public void getACL(String path, Stat stat, ACLCallback cb, Object ctx) {
        wrapped.getACL(path, stat, cb, ctx);
    }

    @Override
    public Stat setACL(String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {
        return wrapped.setACL(path, acl, version);
    }

    @Override
    public void setACL(String path, List<ACL> acl, int version, StatCallback cb, Object ctx) {
        wrapped.setACL(path, acl, version, cb, ctx);
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return wrapped.getChildren(path, watcher);
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        return wrapped.getChildren(path, watch);
    }

    @Override
    public void getChildren(String path, Watcher watcher, ChildrenCallback cb, Object ctx) {
        wrapped.getChildren(path, watcher, cb, ctx);
    }

    @Override
    public void getChildren(String path, boolean watch, ChildrenCallback cb, Object ctx) {
        wrapped.getChildren(path, watch, cb, ctx);
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher, Stat stat) throws KeeperException,
            InterruptedException {
        return wrapped.getChildren(path, watcher, stat);
    }

    @Override
    public List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return wrapped.getChildren(path, watch, stat);
    }

    @Override
    public void getChildren(String path, Watcher watcher, Children2Callback cb, Object ctx) {
        wrapped.getChildren(path, watcher, cb, ctx);
    }

    @Override
    public void getChildren(String path, boolean watch, Children2Callback cb, Object ctx) {
        wrapped.getChildren(path, watch, cb, ctx);
    }

    @Override
    public void sync(String path, VoidCallback cb, Object ctx) {
        wrapped.sync(path, cb, ctx);
    }

    @Override
    public States getState() {
        return wrapped.getState();
    }

}