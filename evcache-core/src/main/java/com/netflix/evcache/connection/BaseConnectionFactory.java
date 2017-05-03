package com.netflix.evcache.connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.netflix.config.ChainedDynamicProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.evcache.EVCacheTranscoder;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPool;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.pool.EVCacheKetamaNodeLocatorConfiguration;
import com.netflix.evcache.pool.EVCacheNodeLocator;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.EVCacheConnection;
import net.spy.memcached.FailureMode;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.binary.EVCacheNodeImpl;
import net.spy.memcached.transcoders.Transcoder;

public class BaseConnectionFactory extends BinaryConnectionFactory {

    protected final String name;
    protected final DynamicIntProperty operationTimeout;
    protected final long opMaxBlockTime;
    protected EVCacheNodeLocator locator;
    protected final long startTime;
    protected final EVCacheClient client;
    protected final ChainedDynamicProperty.StringProperty failureMode;

    BaseConnectionFactory(EVCacheClient client, int len, DynamicIntProperty _operationTimeout, long opMaxBlockTime) {
        super(len, BinaryConnectionFactory.DEFAULT_READ_BUFFER_SIZE, DefaultHashAlgorithm.KETAMA_HASH);
        this.operationTimeout = _operationTimeout;
        this.opMaxBlockTime = opMaxBlockTime;
        this.client = client;
        this.startTime = System.currentTimeMillis();

        final String appName = client.getAppName();
        final int id = client.getId();
        final ServerGroup serverGroup = client.getServerGroup();
        this.failureMode = EVCacheConfig.getInstance().getChainedStringProperty(serverGroup.getName() + ".failure.mode", appName + ".failure.mode", "Retry");
        this.name = appName + "-" + serverGroup.getName() + "-" + id;
    }

    public NodeLocator createLocator(List<MemcachedNode> list) {
        this.locator = new EVCacheNodeLocator(client, list, 
                DefaultHashAlgorithm.KETAMA_HASH, new EVCacheKetamaNodeLocatorConfiguration(client));
        return locator;
    }

    public EVCacheNodeLocator getEVCacheNodeLocator() {
        return this.locator;
    }

    public long getMaxReconnectDelay() {
        return super.getMaxReconnectDelay();
    }

    public int getOpQueueLen() {
        return super.getOpQueueLen();
    }

    public int getReadBufSize() {
        return super.getReadBufSize();
    }

    public BlockingQueue<Operation> createOperationQueue() {
        return new ArrayBlockingQueue<Operation>(getOpQueueLen());
    }

    public MemcachedConnection createConnection(List<InetSocketAddress> addrs) throws IOException {
        return new EVCacheConnection(name, getReadBufSize(), this, addrs, getInitialObservers(), getFailureMode(),
                getOperationFactory());
    }

    public MemcachedNode createMemcachedNode(SocketAddress sa, SocketChannel c, int bufSize) {
        boolean doAuth = false;
        final EVCacheNodeImpl node = new EVCacheNodeImpl(sa, c, bufSize, createReadOperationQueue(),
                createWriteOperationQueue(), createOperationQueue(),
                opMaxBlockTime, doAuth, getOperationTimeout(), getAuthWaitTime(), this, client,
                startTime);
        return node;
    }

    public long getOperationTimeout() {
        return operationTimeout.get();
    }

    public BlockingQueue<Operation> createReadOperationQueue() {
        return super.createReadOperationQueue();
    }

    public BlockingQueue<Operation> createWriteOperationQueue() {
        return super.createWriteOperationQueue();
    }

    public Transcoder<Object> getDefaultTranscoder() {
        return new EVCacheTranscoder();
    }

    public FailureMode getFailureMode() {
        try {
            return FailureMode.valueOf(failureMode.get());
        } catch (IllegalArgumentException ex) {
            return FailureMode.Cancel;
        }
    }

    public HashAlgorithm getHashAlg() {
        return super.getHashAlg();
    }

    public Collection<ConnectionObserver> getInitialObservers() {
        return super.getInitialObservers();
    }

    public boolean isDaemon() {
        return EVCacheConfig.getInstance().getDynamicBooleanProperty("evcache.thread.daemon", super.isDaemon()).get();
    }

    public boolean shouldOptimize() {
        return true;
    }

    public int getId() {
        return client.getId();
    }

    public String getZone() {
        return client.getServerGroup().getZone();
    }

    public String getReplicaSetName() {
        return client.getServerGroup().getName();
    }

    public String toString() {
        return name;
    }

    public EVCacheClientPoolManager getEVCacheClientPoolManager() {
        return this.client.getPool().getEVCacheClientPoolManager();
    }

    public EVCacheClientPool getEVCacheClientPool() {
        return this.client.getPool();
    }

    public EVCacheClient getEVCacheClient() {
        return this.client;
    }
}