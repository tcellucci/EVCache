package com.netflix.evcache.connection;

import javax.inject.Inject;

import com.netflix.evcache.config.CacheConfig;
import com.netflix.evcache.config.CacheConfig.ClusterConfig;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.pool.ServerGroup;

import net.spy.memcached.ConnectionFactory;

public class ConnectionFactoryProvider implements IConnectionFactoryProvider {
    private final CacheConfig cacheConfig;

    @Inject
    public ConnectionFactoryProvider(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    public ConnectionFactory getConnectionFactory(String appName, int id, ServerGroup serverGroup,
            EVCacheClientPoolManager poolManager) {
        ClusterConfig clusterConfig = cacheConfig.getClusterConfig(appName);
        return new BaseConnectionFactory(clusterConfig, appName, id, serverGroup, poolManager);
    }

}
