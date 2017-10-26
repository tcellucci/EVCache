package com.netflix.evcache.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

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
        repo.onChange(someProp, callback);
    }

    @Override
    public Supplier<Long> getMutateTimeout(Long defaultValue) {
        return repo.getProperty("evache.mutate.timeout", defaultValue);
    }

    @Override
    public Supplier<Integer> getDefaultReadTimeout() {
        return repo.getProperty("default.read.timeout", 20);
    }

    public Supplier<String> getLogEnabledApps() {
        return repo.getProperty("EVCacheClientPoolManager.log.apps", "*");
    }

    public Supplier<Boolean> isUseSimpleNodeListProvider() {
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
    public Supplier<Integer> getMetricsSampleSize() {
        return repo.getProperty("EVCache.metrics.sample.size", 100);
    }

    @Override
    public Supplier<Boolean> isEnableThrottleOperations() {
        return repo.getProperty("EVCacheThrottler.throttle.operations", false);
    }

    class DefaultExecutorConfig implements ExecutorConfig {
        private final String name;

        public DefaultExecutorConfig(String name) {
            this.name = name;
        }

        @Override
        public Supplier<Integer> getMaxSize(int defaultValue) {
            return repo.getProperty("EVCacheScheduledExecutor." + name + ".max.size", defaultValue);
        }

        @Override
        public Supplier<Integer> getCoreSize(int defaultValue) {
            return repo.getProperty("EVCacheScheduledExecutor." + name + ".core.size", defaultValue);
        }

        @Override
        public void addCallback(Supplier<?> someProp, Runnable callback) {
           repo.onChange(someProp, callback);
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
            this.throttlerConfig = new SupplierertyRepoThrottlerConfig();
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
            repo.onChange(someProp, callback);
        }

        @Override
        public Supplier<Integer> getMaxDataSizeDefault() {
            return repo.getProperty("default.evcache.max.data.size", Integer.MAX_VALUE);
        }

        @Override
        public Supplier<Integer> getCompressionThresholdDefault() {
            return repo.getProperty("default.evcache.compression.threshold", 120);
        }

        @Override
        public Supplier<Boolean> isSendMetrics() {
            return repo.getProperty("EVCacheNodeImpl." + appName + ".sendMetrics", false);
        }

        @Override
        public Supplier<Integer> getBucketSize(String serverGroupName, Integer defaultValue) {
            return repo.getProperty(appName + "." + serverGroupName + ".bucket.size", appName + ".bucket.size",
                    defaultValue);
        }

        @Override
        public Supplier<Boolean> isNodeLocatorHashOnPartialKey(String serverGroupName) {
            String overrideKey = "EVCacheNodeLocator." + appName + ".hash.on.partial.key";
            String primaryKey = "EVCacheNodeLocator." + appName + "." + serverGroupName + ".hash.on.partial.key";
            Boolean defaultValue = Boolean.FALSE;
            return repo.getProperty(overrideKey, primaryKey, defaultValue);
        }

        @Override
        public Supplier<String> getNodeLocatorHashDelimiter(String serverGroupName) {
            String overrideKey = "EVCacheNodeLocator." + appName + ".hash.delimiter";
            String primaryKey = "EVCacheNodeLocator." + appName + "." + serverGroupName + ".hash.delimiter";
            String defaultValue = ":";
            return repo.getProperty(overrideKey, primaryKey, defaultValue);
        }

        @Override
        public Supplier<Boolean> isAddOperationFixup(boolean defaultVAlue) {
            return repo.getProperty(appName + ".addOperation.fixup", Boolean.FALSE);
        }

        @Override
        public Supplier<Integer> getAddOperationFixupPoolSize(int defaultValue) {
            return repo.getProperty(appName + ".addOperation.fixup.poolsize", 10);
        }

        @Override
        public Supplier<Boolean> isUseSimpleNodeListProvider() {
            return repo.getProperty(appName + ".use.simple.node.list.provider", "evcache.use.simple.node.list.provider",
                    Boolean.FALSE);
        }

        @Override
        public Supplier<String> getFailureMode(ServerGroup serverGroup) {
            return repo.getProperty(serverGroup.getName() + ".failure.mode", appName + ".failure.mode", "Retry");
        }

        @Override
        public Supplier<Boolean> isChunkingEnabled(String serverGroupName) {
            return repo.getProperty(serverGroupName + ".chunk.data", appName + ".chunk.data", Boolean.FALSE);
        }

        @Override
        public Supplier<Integer> getChunkDataSize(String serverGroupName) {
            return repo.getProperty(serverGroupName + ".chunk.size", appName + ".chunk.size", 1180);
        }

        @Override
        public Supplier<Integer> getWriteBlockDuration(String serverGroupName) {
            return repo.getProperty(appName + "." + serverGroupName + ".write.block.duration",
                    appName + ".write.block.duration", 25);
        }

        @Override
        public Supplier<Boolean> isIgnoreTouch(String serverGroupName) {
            return repo.getProperty(appName + "." + serverGroupName + ".ignore.touch", appName + ".ignore.touch",
                    false);
        }

        @Override
        public Supplier<Boolean> isIgnoreInactiveNodes(String serverGroupName) {
            return repo.getProperty(appName + ".ignore.inactive.nodes", false);
        }

        @Override
        public Supplier<Set<String>> getIgnoreHosts() {
            return repo.getProperty(appName + ".ignore.hosts", Collections.emptySet());
        }

        @Override
        public Supplier<Boolean> isUseBatchPort() {
            return repo.getProperty(appName + ".use.batch.port", "evcache.use.batch.port", Boolean.FALSE);
        }

        @Override
        public Supplier<String> getSimpleNodeList() {
            return repo.getProperty(appName + "-NODES", "");
        }

        @Override
        public Supplier<Boolean> isThrowException(String cacheName) {
            return repo.getProperty(appName + "." + cacheName + ".throw.exception", appName + ".throw.exception",
                    Boolean.FALSE);
        }

        @Override
        public Supplier<Boolean> isFallbackZone(String cacheName) {
            return repo.getProperty(appName + "." + cacheName + ".fallback.zone", appName + ".fallback.zone",
                    Boolean.TRUE);
        }

        @Override
        public Supplier<Boolean> isBulkFallbackZone() {
            return repo.getProperty(appName + ".bulk.fallback.zone", Boolean.TRUE);
        }

        @Override
        public Supplier<Boolean> isBulkPartialFallbackZone() {
            return repo.getProperty(appName + ".bulk.partial.fallback.zone", Boolean.TRUE);
        }

        @Override
        public Supplier<Boolean> isUseInMemoryCache() {
            return repo.getProperty(appName + ".use.inmemory.cache", "evcache.use.inmemory.cache", Boolean.FALSE);
        }

        @Override
        public Supplier<Boolean> isEventsUsingLatch() {
            return repo.getProperty(appName + ".events.using.latch", "evcache.events.using.latch", Boolean.FALSE);
        }

        class SupplierertyRepoThrottlerConfig implements ThrottlerConfig {
            @Override
            public Supplier<Boolean> isEnableThrottleHotKeys() {
                return repo.getProperty("EVCacheThrottler." + appName + ".throttle.hot.keys", false);
            }

            @Override
            public Supplier<Integer> getInMemoryExpireCacheSize() {
                return repo.getProperty("EVCacheThrottler." + appName + ".inmemory.cache.size", 100);
            }

            @Override
            public Supplier<Integer> getInMemoryExpireAfterWriteDurationMs() {
                return repo.getProperty("EVCacheThrottler." + appName + ".inmemory.expire.after.write.duration.ms",
                        10000);
            }

            @Override
            public Supplier<Integer> getInMemoryExpireAfterAccessDurationMs() {
                return repo.getProperty("EVCacheThrottler." + appName + ".inmemory.cache.size", 100);
            }

            @Override
            public Supplier<Integer> getThrottlerValue() {
                return repo.getProperty("EVCacheThrottler." + appName + ".throttle.value", 3);
            }

            @Override
            public Supplier<Set<String>> getThrottleKeys() {
                return repo.getProperty(appName + ".throttle.keys", Collections.emptySet());
            }

            @Override
            public Supplier<Set<String>> getThrottleCalls() {
                return repo.getProperty(appName + "throttle.calls", Collections.emptySet());
            }

        }

        class DefaultClientPoolConfig implements ClientPoolConfig {

            @Override
            public Supplier<Integer> getPoolSize() {
                return repo.getProperty(appName + ".EVCacheClientPool.poolSize", 1);
            }

            @Override
            public Supplier<Integer> getReadTimeout() {
                return repo.getProperty(appName + ".EVCacheClientPool.readTimeout", "default.read.timeout", 20);
            }

            @Override
            public Supplier<Integer> getBulkReadTimeout(Supplier<Integer> defaultValue) {
                return repo.getProperty(appName + ".EVCacheClientPool.bulkReadTimeout", defaultValue);
            }

            @Override
            public Supplier<Boolean> isRefreshConnectionOnReadQueueFull() {
                return repo.getProperty(appName + ".EVCacheClientPool.refresh.connection.on.readQueueFull",
                        "EVCacheClientPool.refresh.connection.on.readQueueFull", Boolean.FALSE);
            }

            @Override
            public Supplier<Integer> getRefreshConnectionOnReadQueueFullSize() {
                return repo.getProperty(appName + ".EVCacheClientPool.refresh.connection.on.readQueueFull.size",
                        "EVCacheClientPool.refresh.connection.on.readQueueFull.size", 100);
            }

            @Override
            public Supplier<Integer> getOperationQueueMaxBlockTime() {
                return repo.getProperty(appName + ".operation.QueueMaxBlockTime", 10);
            }

            @Override
            public Supplier<Integer> getOperationTimeout() {
                return repo.getProperty(appName + ".operation.timeout", 2500);
            }

            @Override
            public Supplier<Integer> getMaxReadQueueSize() {
                return repo.getProperty(appName + ".max.read.queue.length", 5);
            }

            @Override
            public Supplier<Boolean> isRetryAllCopies() {
                return repo.getProperty(appName + ".retry.all.copies", Boolean.FALSE);
            }

            @Override
            public Supplier<Boolean> isDisableAsyncRefresh() {
                return repo.getProperty(appName + ".disable.async.refresh", Boolean.FALSE);
            }

            @Override
            public Supplier<Integer> getMaxRetryCount() {
                return repo.getProperty(appName + ".max.retry.count", 1);
            }

            @Override
            public Supplier<Integer> getLogOperations() {
                return repo.getProperty(appName + ".log.operation", 0);
            }

            private final Set<String> OPERATION_CALLS = new HashSet<>(
                    Arrays.asList("SET", "DELETE", "GMISS", "TMISS", "BMISS_ALL", "TOUCH", "REPLACE"));

            @Override
            public Supplier<Set<String>> getLogOperationCalls() {
                return repo.getProperty(appName + ".log.operation.calls", OPERATION_CALLS);
            }

            @Override
            public Supplier<Integer> getReconcileInterval() {
                return repo.getProperty(appName + ".reconcile.interval", 600000);
            }

            @Override
            public Supplier<Set<String>> getCloneWritesTo() {
                return repo.getProperty(appName + ".clone.writes.to", Collections.emptySet());
            }

            @Override
            public Supplier<Boolean> isPingServers() {
                return repo.getProperty(appName + ".ping.servers", "evcache.ping.servers", Boolean.FALSE);
            }

            @Override
            public Supplier<Integer> getMaxQueueLength() {
                return repo.getProperty(appName + ".max.queue.length", 16384);
            }

            @Override
            public Supplier<Boolean> isWriteOnly(ServerGroup serverGroup) {
                return repo.getProperty(appName + "." + serverGroup.getName() + ".EVCacheClientPool.writeOnly",
                        appName + "." + serverGroup.getZone() + ".EVCacheClientPool.writeOnly", Boolean.FALSE);
            }

            @Override
            public Supplier<Boolean> isDaemonMode() {
                return repo.getProperty("evcache.thread.daemon", false);
            }
        }

        class DefaultInMemoryCacheConfig implements InMemoryCacheConfig {

            @Override
            public Supplier<Integer> getCacheDuration() {
                return repo.getProperty(appName + ".inmemory.cache.duration.ms",
                        appName + ".inmemory.expire.after.write.duration.ms", 0);
            }

            @Override
            public Supplier<Integer> getExpireAfterAccessDuration() {
                return repo.getProperty(appName + ".inmemory.expire.after.access.duration.ms", 0);
            }

            @Override
            public Supplier<Integer> getRefreshDuration() {
                return repo.getProperty(appName + ".inmemory.refresh.after.write.duration.ms", 0);
            }

            @Override
            public Supplier<Integer> getCacheSize() {
                return repo.getProperty(appName + ".inmemory.cache.size", 100);
            }

            @Override
            public Supplier<Integer> getPoolSize() {
                return repo.getProperty(appName + ".thread.pool.size", 5);
            }

        }

    }

    @Override
    public Supplier<Boolean> isEnableThrottleHotKeys() {
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