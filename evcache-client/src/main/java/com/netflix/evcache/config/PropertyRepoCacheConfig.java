package com.netflix.evcache.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import com.netflix.evcache.config.PropertyRepo.Prop;
import com.netflix.evcache.pool.ServerGroup;

public class PropertyRepoCacheConfig implements CacheConfig {
    private final ConcurrentMap<String, ClusterConfig> clusterConfigIndex = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ExecutorConfig> executorConfigIndex = new ConcurrentHashMap<>();
    protected final PropertyRepo repo;

    public PropertyRepoCacheConfig(PropertyRepo repo) {
        this.repo = repo;
    }

    @Override
    public void addCallback(Supplier<?> someProp, Runnable callback) {
        if (someProp instanceof Prop) {
            ((Prop<?>) someProp).onChange(callback);
        }
    }

    @Override
    public Prop<Long> getMutateTimeout(Long defaultValue) {
        return repo.getProperty("evache.mutate.timeout", defaultValue);
    }

    @Override
    public Prop<Integer> getDefaultReadTimeout() {
        return repo.getProperty("default.read.timeout", 20);
    }

    public Prop<String> getLogEnabledApps() {
        return repo.getProperty("EVCacheClientPoolManager.log.apps", "*");
    }

    public Prop<Boolean> isUseSimpleNodeListProvider() {
        return repo.getProperty("evcache.use.simple.node.list.provider", false);
    }

    public String getAppsToInit() {
        return repo.getProperty("evcache.appsToInit", "").get();
    }

    @Override
    public String getAlias(String appId) {
        return repo.getProperty("EVCacheClientPoolManager." + appId + ".alias", appId).get();
    }

    @Override
    public Prop<Integer> getMetricsSampleSize() {
        return repo.getProperty("EVCache.metrics.sample.size", 100);
    }

    @Override
    public Prop<Boolean> isEnableThrottleOperations() {
        return repo.getProperty("EVCacheThrottler.throttle.operations", false);
    }

    class DefaultExecutorConfig implements ExecutorConfig {
        private final String name;

        public DefaultExecutorConfig(String name) {
            this.name = name;
        }

        @Override
        public Prop<Integer> getMaxSize(int defaultValue) {
            return repo.getProperty("EVCacheScheduledExecutor." + name + ".max.size", defaultValue);
        }

        @Override
        public Prop<Integer> getCoreSize(int defaultValue) {
            return repo.getProperty("EVCacheScheduledExecutor." + name + ".core.size", defaultValue);
        }

        @Override
        public void addCallback(Supplier<?> someProp, Runnable callback) {
            PropertyRepoCacheConfig.this.addCallback(someProp, callback);
        }

    }

    class DefaultClusterConfig implements ClusterConfig {
        private final String appName;
        private final ClientPoolConfig clientPoolConfig;
        private final InMemoryCacheConfig inMemoryCacheConfig;
        private final ThrottlerConfig throttlerConfig;

        DefaultClusterConfig(String appId) {
            this.appName = appId;
            this.clientPoolConfig = new DefaultClientPoolConfig();
            this.inMemoryCacheConfig = new DefaultInMemoryCacheConfig();
            this.throttlerConfig = new PropertyRepoThrottlerConfig();
        }

        @Override
        public String getClusterName() {
            return appName;
        }

        @Override
        public ClientPoolConfig getClientPoolConfig() {
            return clientPoolConfig;
        }

        @Override
        public InMemoryCacheConfig getInMemoryCacheConfig() {
            return inMemoryCacheConfig;
        }

        @Override
        public ThrottlerConfig getThrottlerConfig() {
            return throttlerConfig;
        }

        @Override
        public void addCallback(Supplier<?> someProp, Runnable callback) {
            PropertyRepoCacheConfig.this.addCallback(someProp, callback);
        }

        @Override
        public Prop<Integer> getMaxDataSizeDefault() {
            return repo.getProperty("default.evcache.max.data.size", Integer.MAX_VALUE);
        }

        @Override
        public Prop<Integer> getCompressionThresholdDefault() {
            return repo.getProperty("default.evcache.compression.threshold", 120);
        }

        @Override
        public Prop<Boolean> isSendMetrics() {
            return repo.getProperty("EVCacheNodeImpl." + appName + ".sendMetrics", false);
        }

        @Override
        public Prop<Integer> getBucketSize(String serverGroupName, Integer defaultValue) {
            return repo.getProperty(appName + "." + serverGroupName + ".bucket.size", appName + ".bucket.size",
                    defaultValue);
        }

        @Override
        public Prop<Boolean> isNodeLocatorHashOnPartialKey(String serverGroupName) {
            String overrideKey = "EVCacheNodeLocator." + appName + ".hash.on.partial.key";
            String primaryKey = "EVCacheNodeLocator." + appName + "." + serverGroupName + ".hash.on.partial.key";
            Boolean defaultValue = Boolean.FALSE;
            return repo.getProperty(overrideKey, primaryKey, defaultValue);
        }

        @Override
        public Prop<String> getNodeLocatorHashDelimiter(String serverGroupName) {
            String overrideKey = "EVCacheNodeLocator." + appName + ".hash.delimiter";
            String primaryKey = "EVCacheNodeLocator." + appName + "." + serverGroupName + ".hash.delimiter";
            String defaultValue = ":";
            return repo.getProperty(overrideKey, primaryKey, defaultValue);
        }

        @Override
        public Prop<Boolean> isAddOperationFixup(boolean defaultVAlue) {
            return repo.getProperty(appName + ".addOperation.fixup", Boolean.FALSE);
        }

        @Override
        public Prop<Integer> getAddOperationFixupPoolSize(int defaultValue) {
            return repo.getProperty(appName + ".addOperation.fixup.poolsize", 10);
        }

        @Override
        public Prop<Boolean> isUseSimpleNodeListProvider() {
            return repo.getProperty(appName + ".use.simple.node.list.provider", "evcache.use.simple.node.list.provider",
                    Boolean.FALSE);
        }

        @Override
        public Prop<String> getFailureMode(ServerGroup serverGroup) {
            return repo.getProperty(serverGroup.getName() + ".failure.mode", appName + ".failure.mode", "Retry");
        }

        @Override
        public Prop<Boolean> isChunkingEnabled(String serverGroupName) {
            return repo.getProperty(serverGroupName + ".chunk.data", appName + ".chunk.data", Boolean.FALSE);
        }

        @Override
        public Prop<Integer> getChunkDataSize(String serverGroupName) {
            return repo.getProperty(serverGroupName + ".chunk.size", appName + ".chunk.size", 1180);
        }

        @Override
        public Prop<Integer> getWriteBlockDuration(String serverGroupName) {
            return repo.getProperty(appName + "." + serverGroupName + ".write.block.duration",
                    appName + ".write.block.duration", 25);
        }

        @Override
        public Prop<Boolean> isIgnoreTouch(String serverGroupName) {
            return repo.getProperty(appName + "." + serverGroupName + ".ignore.touch", appName + ".ignore.touch",
                    false);
        }

        @Override
        public Prop<Boolean> isIgnoreInactiveNodes(String serverGroupName) {
            return repo.getProperty(appName + ".ignore.inactive.nodes", false);
        }

        @Override
        public Prop<Set<String>> getIgnoreHosts() {
            return repo.getProperty(appName + ".ignore.hosts", Collections.emptySet());
        }

        @Override
        public Prop<Boolean> isUseBatchPort() {
            return repo.getProperty(appName + ".use.batch.port", "evcache.use.batch.port", Boolean.FALSE);
        }

        @Override
        public Prop<String> getSimpleNodeList() {
            return repo.getProperty(appName + "-NODES", "");
        }

        @Override
        public Prop<Boolean> isThrowException(String cacheName) {
            return repo.getProperty(appName + "." + cacheName + ".throw.exception", appName + ".throw.exception",
                    Boolean.FALSE);
        }

        @Override
        public Prop<Boolean> isFallbackZone(String cacheName) {
            return repo.getProperty(appName + "." + cacheName + ".fallback.zone", appName + ".fallback.zone",
                    Boolean.TRUE);
        }

        @Override
        public Prop<Boolean> isBulkFallbackZone() {
            return repo.getProperty(appName + ".bulk.fallback.zone", Boolean.TRUE);
        }

        @Override
        public Prop<Boolean> isBulkPartialFallbackZone() {
            return repo.getProperty(appName + ".bulk.partial.fallback.zone", Boolean.TRUE);
        }

        @Override
        public Prop<Boolean> isUseInMemoryCache() {
            return repo.getProperty(appName + ".use.inmemory.cache", "evcache.use.inmemory.cache", Boolean.FALSE);
        }

        @Override
        public Prop<Boolean> isEventsUsingLatch() {
            return repo.getProperty(appName + ".events.using.latch", "evcache.events.using.latch", Boolean.FALSE);
        }

        class PropertyRepoThrottlerConfig implements ThrottlerConfig {
            @Override
            public Prop<Boolean> isEnableThrottleHotKeys() {
                return repo.getProperty("EVCacheThrottler." + appName + ".throttle.hot.keys", false);
            }

            @Override
            public Prop<Integer> getInMemoryExpireCacheSize() {
                return repo.getProperty("EVCacheThrottler." + appName + ".inmemory.cache.size", 100);
            }

            @Override
            public Prop<Integer> getInMemoryExpireAfterWriteDurationMs() {
                return repo.getProperty("EVCacheThrottler." + appName + ".inmemory.expire.after.write.duration.ms",
                        10000);
            }

            @Override
            public Prop<Integer> getInMemoryExpireAfterAccessDurationMs() {
                return repo.getProperty("EVCacheThrottler." + appName + ".inmemory.cache.size", 100);
            }

            @Override
            public Prop<Integer> getThrottlerValue() {
                return repo.getProperty("EVCacheThrottler." + appName + ".throttle.value", 3);
            }

            @Override
            public Prop<Set<String>> getThrottleKeys() {
                return repo.getProperty(appName + ".throttle.keys", Collections.emptySet());
            }

            @Override
            public Prop<Set<String>> getThrottleCalls() {
                return repo.getProperty(appName + "throttle.calls", Collections.emptySet());
            }

        }

        class DefaultClientPoolConfig implements ClientPoolConfig {

            @Override
            public Prop<Integer> getPoolSize() {
                return repo.getProperty(appName + ".EVCacheClientPool.poolSize", 1);
            }

            @Override
            public Prop<Integer> getReadTimeout() {
                return repo.getProperty(appName + ".EVCacheClientPool.readTimeout", "default.read.timeout", 20);
            }

            @Override
            public Prop<Integer> getBulkReadTimeout(Supplier<Integer> defaultValue) {
                return repo.getProperty(appName + ".EVCacheClientPool.bulkReadTimeout", defaultValue);
            }

            @Override
            public Prop<Boolean> isRefreshConnectionOnReadQueueFull() {
                return repo.getProperty(appName + ".EVCacheClientPool.refresh.connection.on.readQueueFull",
                        "EVCacheClientPool.refresh.connection.on.readQueueFull", Boolean.FALSE);
            }

            @Override
            public Prop<Integer> getRefreshConnectionOnReadQueueFullSize() {
                return repo.getProperty(appName + ".EVCacheClientPool.refresh.connection.on.readQueueFull.size",
                        "EVCacheClientPool.refresh.connection.on.readQueueFull.size", 100);
            }

            @Override
            public Prop<Integer> getOperationQueueMaxBlockTime() {
                return repo.getProperty(appName + ".operation.QueueMaxBlockTime", 10);
            }

            @Override
            public Prop<Integer> getOperationTimeout() {
                return repo.getProperty(appName + ".operation.timeout", 2500);
            }

            @Override
            public Prop<Integer> getMaxReadQueueSize() {
                return repo.getProperty(appName + ".max.read.queue.length", 5);
            }

            @Override
            public Prop<Boolean> isRetryAllCopies() {
                return repo.getProperty(appName + ".retry.all.copies", Boolean.FALSE);
            }

            @Override
            public Prop<Boolean> isDisableAsyncRefresh() {
                return repo.getProperty(appName + ".disable.async.refresh", Boolean.FALSE);
            }

            @Override
            public Prop<Integer> getMaxRetryCount() {
                return repo.getProperty(appName + ".max.retry.count", 1);
            }

            @Override
            public Prop<Integer> getLogOperations() {
                return repo.getProperty(appName + ".log.operation", 0);
            }

            private final Set<String> OPERATION_CALLS = new HashSet<>(
                    Arrays.asList("SET", "DELETE", "GMISS", "TMISS", "BMISS_ALL", "TOUCH", "REPLACE"));

            @Override
            public Prop<Set<String>> getLogOperationCalls() {
                return repo.getProperty(appName + ".log.operation.calls", OPERATION_CALLS);
            }

            @Override
            public Prop<Integer> getReconcileInterval() {
                return repo.getProperty(appName + ".reconcile.interval", 600000);
            }

            @Override
            public Prop<Set<String>> getCloneWritesTo() {
                return repo.getProperty(appName + ".clone.writes.to", Collections.emptySet());
            }

            @Override
            public Prop<Boolean> isPingServers() {
                return repo.getProperty(appName + ".ping.servers", "evcache.ping.servers", Boolean.FALSE);
            }

            @Override
            public Prop<Integer> getMaxQueueLength() {
                return repo.getProperty(appName + ".max.queue.length", 16384);
            }

            @Override
            public Prop<Boolean> isWriteOnly(ServerGroup serverGroup) {
                return repo.getProperty(appName + "." + serverGroup.getName() + ".EVCacheClientPool.writeOnly",
                        appName + "." + serverGroup.getZone() + ".EVCacheClientPool.writeOnly", Boolean.FALSE);
            }

            @Override
            public Prop<Boolean> isDaemonMode() {
                return repo.getProperty("evcache.thread.daemon", false);
            }
        }

        class DefaultInMemoryCacheConfig implements InMemoryCacheConfig {

            @Override
            public Prop<Integer> getCacheDuration() {
                return repo.getProperty(appName + ".inmemory.cache.duration.ms",
                        appName + ".inmemory.expire.after.write.duration.ms", 0);
            }

            @Override
            public Prop<Integer> getExpireAfterAccessDuration() {
                return repo.getProperty(appName + ".inmemory.expire.after.access.duration.ms", 0);
            }

            @Override
            public Prop<Integer> getRefreshDuration() {
                return repo.getProperty(appName + ".inmemory.refresh.after.write.duration.ms", 0);
            }

            @Override
            public Prop<Integer> getCacheSize() {
                return repo.getProperty(appName + ".inmemory.cache.size", 100);
            }

            @Override
            public Prop<Integer> getPoolSize() {
                return repo.getProperty(appName + ".thread.pool.size", 5);
            }

        }

    }

    @Override
    public Prop<Boolean> isEnableThrottleHotKeys() {
        return repo.getProperty("EVCacheThrottler.throttle.hot.keys", false);
    }

    @Override
    public ClusterConfig getClusterConfig(String appName) {
        return clusterConfigIndex.computeIfAbsent(appName, n -> new DefaultClusterConfig(n));
    }

    @Override
    public ExecutorConfig getExecutorConfig(String executorName) {
        return executorConfigIndex.computeIfAbsent(executorName, n -> new DefaultExecutorConfig(n));
    }

}