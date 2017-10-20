package com.netflix.evcache;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoOptional;
import com.netflix.archaius.api.annotations.ConfigurationSource;
import com.netflix.evcache.config.Archaius1PropertyRepo;
import com.netflix.evcache.config.CacheConfig;
import com.netflix.evcache.config.PropertyRepoCacheConfig;
import com.netflix.evcache.event.hotkey.HotKeyListener;
import com.netflix.evcache.event.throttle.ThrottleListener;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.EVCacheClientPoolManager;

@Singleton
public class EVCacheModule extends AbstractModule {

    public EVCacheModule() {
    }

    @Singleton
    @ConfigurationSource("evcache")
    public static class EVCacheModuleConfigLoader {
    }


    @Override
    protected void configure() {
        bind(EVCacheModuleConfigLoader.class).asEagerSingleton();
        bind(EVCacheClientPoolManager.class).asEagerSingleton();
        
        bind(HotKeyListener.class).asEagerSingleton();
        bind(ThrottleListener.class).asEagerSingleton();
        

        // Make sure connection factory provider Module is initialized in your Module when you init EVCacheModule 
        // bind(IConnectionFactoryProvider.class).toProvider(DefaultFactoryProvider.class);
    }
    
    @Provides
    public CacheConfig archaius1CacheConfig() {
    	return new PropertyRepoCacheConfig(new Archaius1PropertyRepo());
    }
    
    @Provides
    public EVCacheMetricsFactory cacheMetricsFactory(CacheConfig cacheConfig) {
    	return new EVCacheMetricsFactory(cacheConfig.getMetricsSampleSize());
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
 
    @Override
    public boolean equals(Object obj) {
        return (obj != null) && (obj.getClass() == getClass());
    }

}