package com.netflix.evcache.config;

import java.util.Set;
import java.util.function.Supplier;

import com.netflix.evcache.pool.ServerGroup;

public interface CacheConfig {

	ClusterConfig getClusterConfig(String appName);

	ExecutorConfig getExecutorConfig(String name);
	
	void addCallback(Supplier<?> someProp, Runnable callback);

	Supplier<Long> getMutateTimeout(Long defaultValue); // evache.mutate.timeout

	Supplier<Integer> getDefaultReadTimeout();

	Supplier<String> getLogEnabledApps();

	Supplier<Boolean> isUseSimpleNodeListProvider();

	String getAppsToInit();

	String getAlias(String appId);

	Supplier<Integer> getMetricsSampleSize();

	Supplier<Boolean> isEnableThrottleOperations();

	Supplier<Boolean> isEnableThrottleHotKeys();

	interface ExecutorConfig {
		Supplier<Integer> getMaxSize(int defaultValue);

		Supplier<Integer> getCoreSize(int defaultValue);
	}

	public interface ClusterConfig {
		String getClusterName();

		ClientPoolConfig getClientPoolConfig();

		InMemoryCacheConfig getInMemoryCacheConfig();

		ThrottlerConfig getThrottlerConfig();
		
		void addCallback(Supplier<?> someProp, Runnable callback);

		Supplier<Integer> getMaxDataSizeDefault();

		Supplier<Integer> getCompressionThresholdDefault();

		Supplier<Boolean> isSendMetrics();

		Supplier<String> getSimpleNodeList();

		Supplier<Integer> getBucketSize(String serverGroupName, Integer defaultValue);

		Supplier<Boolean> isNodeLocatorHashOnPartialKey(String serverGroupName);

		Supplier<String> getNodeLocatorHashDelimiter(String serverGroupName);

		Supplier<Boolean> isAddOperationFixup(boolean defaultValue);

		Supplier<Integer> getAddOperationFixupPoolSize(int defaultValue);

		Supplier<Boolean> isUseSimpleNodeListProvider();

		Supplier<Boolean> isChunkingEnabled(String serverGroupName);

		Supplier<Integer> getChunkDataSize(String serverGroupName);

		Supplier<Integer> getWriteBlockDuration(String serverGroupName);

		Supplier<Boolean> isIgnoreTouch(String serverGroupName);

		Supplier<Boolean> isIgnoreInactiveNodes(String serverGroupName);

		Supplier<Boolean> isUseBatchPort();

		Supplier<Set<String>> getIgnoreHosts();

		Supplier<String> getFailureMode(ServerGroup serverGroup);

		Supplier<Boolean> isThrowException(String cacheName);

		Supplier<Boolean> isFallbackZone(String cacheName);

		Supplier<Boolean> isBulkFallbackZone();

		Supplier<Boolean> isBulkPartialFallbackZone();

		Supplier<Boolean> isUseInMemoryCache();

		Supplier<Boolean> isEventsUsingLatch();

		public interface ThrottlerConfig {

			Supplier<Boolean> isEnableThrottleHotKeys();

			Supplier<Integer> getInMemoryExpireCacheSize();

			Supplier<Integer> getInMemoryExpireAfterWriteDurationMs();

			Supplier<Integer> getInMemoryExpireAfterAccessDurationMs();

			Supplier<Integer> getThrottlerValue();

			Supplier<Set<String>> getThrottleKeys();

			Supplier<Set<String>> getThrottleCalls();

		}

		public interface InMemoryCacheConfig {
			Supplier<Integer> getCacheDuration();

			Supplier<Integer> getExpireAfterAccessDuration();

			Supplier<Integer> getRefreshDuration();

			Supplier<Integer> getCacheSize();

			Supplier<Integer> getPoolSize();
		}

		public interface ClientPoolConfig {
			Supplier<Integer> getPoolSize();

			Supplier<Integer> getReadTimeout();

			Supplier<Integer> getBulkReadTimeout(Supplier<Integer> defaultTimeout);

			Supplier<Boolean> isRefreshConnectionOnReadQueueFull();

			Supplier<Integer> getRefreshConnectionOnReadQueueFullSize();

			Supplier<Integer> getOperationQueueMaxBlockTime();

			Supplier<Integer> getOperationTimeout();

			Supplier<Integer> getMaxReadQueueSize();

			Supplier<Boolean> isRetryAllCopies();

			Supplier<Boolean> isDisableAsyncRefresh();

			Supplier<Integer> getMaxRetryCount();

			Supplier<Integer> getLogOperations();

			Supplier<Set<String>> getLogOperationCalls();

			Supplier<Integer> getReconcileInterval();

			Supplier<Set<String>> getCloneWritesTo();

			Supplier<Boolean> isPingServers();

			Supplier<Integer> getMaxQueueLength();

			Supplier<Boolean> isWriteOnly(ServerGroup serverGroup);

			Supplier<Boolean> isDaemonMode();

		}
	}
}
