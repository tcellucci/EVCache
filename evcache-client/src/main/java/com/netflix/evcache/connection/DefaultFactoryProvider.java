package com.netflix.evcache.connection;

import javax.inject.Inject;
import javax.inject.Provider;

import com.netflix.evcache.config.CacheConfig;

public class DefaultFactoryProvider implements Provider<IConnectionFactoryProvider> {
	private final CacheConfig cacheConfig;
	@Inject
	public DefaultFactoryProvider(CacheConfig cacheConfig) {
		this.cacheConfig = cacheConfig;
	}

    @Override
    public ConnectionFactoryProvider get() {
        return new ConnectionFactoryProvider(cacheConfig);
    }

}
