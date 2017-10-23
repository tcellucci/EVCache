package com.netflix.evcache.event.hotkey;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.evcache.config.CacheConfig;
import com.netflix.evcache.config.CacheConfig.ClusterConfig.ThrottlerConfig;
import com.netflix.evcache.event.EVCacheEvent;
import com.netflix.evcache.event.EVCacheEventListener;
import com.netflix.evcache.pool.EVCacheClientPoolManager;

/**
 * <p>
 * To enable throttling of requests on the client for keys that are sending too many requests in a short duration then set the below property
 *      <code>EVCacheThrottler.throttle.hot.keys=true</code>
 * </p>
 * <br>
 * Hot keys can be throttled in 2 ways.
 * 
 * <ol>
 * <li>If there are set of keys that are determined by an offline process or enabling debugging then we can set the following property (, separated)
 * 
 *      ex: <code><evcache appName>.throttle.keys=key1,key2</code>
 *      This will throttle all operations for keys key1 and key2
 * 
 * </li><li>Another option is to dynamically figure based on metrics if a key is having a lot of operations. 
 *    At the start of every operation we add the key to an internal cache for a duration specified by <code>EVCacheThrottler.< evcache appName>.inmemory.expire.after.write.duration.ms</code> (default is 10 seconds).
 *    If a key appears again within this duration we increment the value and release the key for <code>EVCacheThrottler.< evcache appName>.inmemory.expire.after.access.duration.ms</code> (default is 10 seconds).
 *    Once the key count crosses <code>EVCacheThrottler.< evcache appName>.throttle.value</code> (default is 3) then the key will be throttled. YMMV so tune this based on your evcache app and client requests.
 *    </li>
 *
 * @author smadappa
 *
 */
@Singleton
public class HotKeyListener implements EVCacheEventListener {

    private static final Logger log = LoggerFactory.getLogger(HotKeyListener.class);
    private final Map<String, Supplier<Boolean>> throttleMap;
    private final Map<String, Cache<String, Integer>> cacheMap;
    private final Integer START_VAL = Integer.valueOf(1);
    private final Supplier<Boolean> enableThrottleHotKeys;
    private final EVCacheClientPoolManager poolManager;
    private final CacheConfig cacheConfig;

    @Inject
    public HotKeyListener(CacheConfig cacheConfig, EVCacheClientPoolManager poolManager) {
        this.cacheConfig = cacheConfig;
        this.poolManager = poolManager;
        this.throttleMap = new ConcurrentHashMap<>();
        cacheMap = new ConcurrentHashMap<String, Cache<String, Integer>>();
        enableThrottleHotKeys = cacheConfig.isEnableThrottleHotKeys();
        cacheConfig.addCallback(enableThrottleHotKeys, this::setupHotKeyListener);
        if(enableThrottleHotKeys.get()) setupHotKeyListener();
    }

    private void setupHotKeyListener() {
        if(enableThrottleHotKeys.get()) {
            poolManager.addEVCacheEventListener(this);
        } else {
            poolManager.removeEVCacheEventListener(this);
            for(Cache<String, Integer> cache : cacheMap.values()) {
                cache.invalidateAll();
            }
        }
    }

    private Cache<String, Integer> getCache(String appName) {
        ThrottlerConfig throttlerConfig = cacheConfig.getClusterConfig(appName).getThrottlerConfig();
        boolean throttleFlag = throttlerConfig.isEnableThrottleHotKeys().get();
        if(log.isDebugEnabled()) log.debug("Throttle hot keys : " + throttleFlag);
        if(!throttleFlag) {
            return null;
        }

        Cache<String, Integer> cache = cacheMap.get(appName);
        if(cache != null) return cache; 
        final int _cacheDuration = throttlerConfig.getInMemoryExpireAfterWriteDurationMs().get();
        final int _exireAfterAccessDuration = throttlerConfig.getInMemoryExpireAfterAccessDurationMs().get();
        final int _cacheSize = throttlerConfig.getInMemoryExpireCacheSize().get();

        CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().recordStats();
        if(_cacheSize > 0) {
            builder = builder.maximumSize(_cacheSize);
        }
        if(_exireAfterAccessDuration > 0) {
            builder = builder.expireAfterAccess(_exireAfterAccessDuration, TimeUnit.MILLISECONDS);
        } else if(_cacheDuration > 0) {
            builder = builder.expireAfterWrite(_cacheDuration, TimeUnit.MILLISECONDS);
        }  
        cache = builder.build();
        cacheMap.put(appName, cache);
        return cache;
    }

    public void onStart(final EVCacheEvent e) {
        if(!enableThrottleHotKeys.get()) return;

        final Cache<String, Integer> cache = getCache(e.getAppName());
        if(cache == null) return;
        for(String key : e.getKeys()) {
            Integer val = cache.getIfPresent(key);
            if(val == null) {
                cache.put(key, START_VAL);
            } else {
                cache.put(key, Integer.valueOf(val.intValue() + 1));
            }
        }
    }

    @Override
    public boolean onThrottle(final EVCacheEvent e) {
        if(!enableThrottleHotKeys.get()) return false;

        final String appName = e.getAppName();
        ThrottlerConfig throttlerConfig = cacheConfig.getClusterConfig(appName).getThrottlerConfig();
        Set<String> throttleKeysSet = throttlerConfig.getThrottleKeys().get();
        if(!throttleKeysSet.isEmpty()) {
            if(log.isDebugEnabled()) log.debug("Throttle : " + throttleKeysSet);
            for(String key : e.getKeys()) {
                if(throttleKeysSet.contains(key)) {
                    if(log.isDebugEnabled()) log.debug("Key : " + key + " is throttled");
                    return true;
                }
            }
        }

        final Cache<String, Integer> cache = getCache(appName);
        if(cache == null) return false;
        int throttleVal = throttlerConfig.getThrottlerValue().get();
        for(String key : e.getKeys()) {
            Integer val = cache.getIfPresent(key);
            if(val.intValue() > throttleVal) {
                if(log.isDebugEnabled()) log.debug("Key : " + key + " has exceeded " + throttleVal + ". Will throttle this request");
                return true;
            }
        }
        return false;
    }

    public void onComplete(EVCacheEvent e) {
        if(!enableThrottleHotKeys.get()) return;
        final String appName = e.getAppName();
        final Cache<String, Integer> cache = getCache(appName);
        if(cache == null) return;

        for(String key : e.getKeys()) {
            Integer val = cache.getIfPresent(key);
            if(val != null) {
                cache.put(key, Integer.valueOf(val.intValue() - 1));
            }
        }
    }

    public void onError(EVCacheEvent e, Throwable t) {
        if(!enableThrottleHotKeys.get()) return;
        final String appName = e.getAppName();
        final Cache<String, Integer> cache = getCache(appName);
        if(cache == null) return;

        for(String key : e.getKeys()) {
            Integer val = cache.getIfPresent(key);
            if(val != null) {
                cache.put(key, Integer.valueOf(val.intValue() - 1));
            }
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cacheMap == null) ? 0 : cacheMap.hashCode());
        result = prime * result + ((throttleMap == null) ? 0 : throttleMap.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HotKeyListener other = (HotKeyListener) obj;
        if (cacheMap == null) {
            if (other.cacheMap != null)
                return false;
        } else if (!cacheMap.equals(other.cacheMap))
            return false;
        if (throttleMap == null) {
            if (other.throttleMap != null)
                return false;
        } else if (!throttleMap.equals(other.throttleMap))
            return false;
        return true;
    }
}