package com.netflix.evcache.test;


import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.multibindings.ProvidesIntoOptional;
import com.google.inject.multibindings.ProvidesIntoOptional.Type;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.Config;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.config.ConfigurationManager;
import com.netflix.discovery.guice.EurekaModule;
import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.EVCacheModule;
import com.netflix.evcache.EVCacheLatch.Policy;
import com.netflix.evcache.config.Archaius2PropertyRepo;
import com.netflix.evcache.config.CacheConfig;
import com.netflix.evcache.config.PropertyRepo;
import com.netflix.evcache.connection.ConnectionModule;
import com.netflix.evcache.operation.EVCacheLatchImpl;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.guice.LifecycleInjectorBuilder;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.spectator.nflx.SpectatorModule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.TestNG;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import rx.Scheduler;

@SuppressWarnings("unused")
public abstract class Base implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Base.class);
    protected EVCache evCache = null;
    protected Injector injector;
    protected LifecycleManager lifecycleManager;
    protected EVCacheClientPoolManager manager = null;

    protected Properties getProps() {
        String hostname = System.getenv("EC2_HOSTNAME");
        Properties props = new Properties();
        if(hostname == null) {
            props.setProperty("eureka.datacenter", "datacenter");//change to ndc while running on desktop
            props.setProperty("eureka.validateInstanceId","false");
            props.setProperty("eureka.mt.connect_timeout","1");
            props.setProperty("eureka.mt.read_timeout","1");
        } else {
            props.setProperty("eureka.datacenter", "cloud");
            props.setProperty("eureka.validateInstanceId","true");
        }

        props.setProperty("eureka.environment", "test");
        props.setProperty("@environment", "test");
        System.setProperty("eureka.region", "us-east-1");
        props.setProperty("eureka.region", "us-east-1");
        props.setProperty("@region", "us-east-1");
        props.setProperty("eureka.appid", "clatency");
        props.setProperty("eureka.serviceUrl.default","http://${@region}.discovery${@environment}.netflix.net:7001/discovery/v2/");
        return props;
    }

    /**
     * load evcache configuration properties via Archaius1
     * @param props
     */
    public void setupTest(Properties props) {
        ConfigurationManager.loadProperties(props);
    }

    /**
     * 
     */
    @BeforeSuite
    public void setupEnv() {
        Properties props = getProps();

        try {
            setupTest(props);

            List<Module> modules = new ArrayList<>();
            modules.addAll(Arrays.asList(new EurekaModule(),
                    new EVCacheModule(), 
                    new ConnectionModule(),
                    new SpectatorModule()));
            modules.addAll(getModules());
            
            LifecycleInjectorBuilder builder = LifecycleInjector.builder().withModules(modules);

            this.injector = builder.build().createInjector();
            this.lifecycleManager = injector.getInstance(LifecycleManager.class);

            this.lifecycleManager.start();
            this.injector.getInstance(ApplicationInfoManager.class);
            final EVCacheModule lib = injector.getInstance(EVCacheModule.class);
            this.manager = injector.getInstance(EVCacheClientPoolManager.class);
        } catch (Throwable e) {
            e.printStackTrace();
            log.error(e.getMessage(), e);
        }

    }
    
    protected List<Module> getModules() {
        return Collections.emptyList();
    }

    @AfterSuite
    public void shutdownEnv() {
        if (lifecycleManager != null) {
            try {
                lifecycleManager.close();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    protected EVCache.Builder getNewBuilder() {
        final EVCache.Builder evCacheBuilder = injector.getInstance(EVCache.Builder.class);
        if(log.isDebugEnabled()) log.debug("evCacheBuilder : " + evCacheBuilder);
        return evCacheBuilder;
    }

    protected boolean append(int i, EVCache gCache) throws Exception {
        String val = ";APP_" + i;
        String key = "key_" + i;
        Future<Boolean>[] status = gCache.append(key, val, 60 * 60);
        for (Future<Boolean> s : status) {
            if (log.isDebugEnabled()) log.debug("APPEND : key : " + key + "; success = " + s.get() + "; Future = " + s.toString());
            if (s.get() == Boolean.FALSE) return false;
        }
        return true;
    }
    
    protected boolean appendOrAdd(int i, EVCache gCache) throws Exception {
        return appendOrAdd(i, gCache, 60 * 60);
    }

    protected boolean appendOrAdd(int i, EVCache gCache, int ttl) throws Exception {
        String val = "val_aa_" + i;
        String key = "key_" + i;
        EVCacheLatch latch = gCache.appendOrAdd(key, val, null, ttl, Policy.ALL_MINUS_1);
        if(log.isDebugEnabled()) log.debug("AppendOrAdd : key : " + key + "; Latch = " + latch);
        boolean status = latch.await(2000, TimeUnit.MILLISECONDS);
        if(log.isDebugEnabled()) log.debug("AppendOrAdd : key : " + key + "; success = " + status);
        return true;
    }

    public boolean add(int i, EVCache gCache) throws Exception {
        //String val = "This is a very long value that should work well since we are going to use compression on it. blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah val_"+i;
        String val = "val_add_"+i;
        String key = "key_" + i;
        boolean status = gCache.add(key, val, null, 60 * 60);
        if(log.isDebugEnabled()) log.debug("ADD : key : " + key + "; success = " + status);
        return status;
    }
    
    public boolean insert(int i, EVCache gCache) throws Exception {
        //String val = "This is a very long value that should work well since we are going to use compression on it. blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah val_"+i;
        String val = "val_"+i;
        String key = "key_" + i;
        Future<Boolean>[] status = gCache.set(key, val, 60 * 60);
        for(Future<Boolean> s : status) {
            if(log.isDebugEnabled()) log.debug("SET : key : " + key + "; success = " + s.get() + "; Future = " + s.toString());
            if(s.get() == Boolean.FALSE) return false;
        }
        return true;
    }
    
    protected boolean replace(int i, EVCache gCache) throws Exception {
        return replace(i, gCache, 60 * 60);
    }

    protected boolean replace(int i, EVCache gCache, int ttl) throws Exception {
        String val = "val_replaced_" + i;
        String key = "key_" + i;
        EVCacheLatch status = gCache.replace(key, val, null, ttl, Policy.ALL);
        boolean opStatus = status.await(1000, TimeUnit.MILLISECONDS);
        if (log.isDebugEnabled()) log.debug("REPLACE : key : " + key + "; success = " + opStatus + "; EVCacheLatch = " + status);
        return status.getSuccessCount() > 0;
    }

    

    public boolean delete(int i, EVCache gCache) throws Exception {
        String key = "key_" + i;
        Future<Boolean>[] status = gCache.delete(key);
        for(Future<Boolean> s : status) {
            if(log.isDebugEnabled()) log.debug("DELETE : key : " + key + "; success = " + s.get() + "; Future = " + s.toString());
            if(s.get() == Boolean.FALSE) return false;
        }
        return true;
    }

    protected boolean touch(int i, EVCache gCache) throws Exception {
        return touch(i, gCache, 60 * 60);
    }

    protected boolean touch(int i, EVCache gCache, int ttl) throws Exception {
        String key = "key_" + i;
        Future<Boolean>[] status = gCache.touch(key, ttl);
        for (Future<Boolean> s : status) {
            if (log.isDebugEnabled()) log.debug("TOUCH : key : " + key + "; success = " + s.get() + "; Future = " + s.toString());
            if (s.get() == Boolean.FALSE) return false;
        }
        return true;
    }

    protected boolean insertUsingLatch(int i, String app) throws Exception {
        String val = "val_" + i;
        String key = "key_" + i;
        long start = System.currentTimeMillis();
        final EVCacheClient[] clients = manager.getEVCacheClientPool(app).getEVCacheClientForWrite();
        final EVCacheLatch latch = new EVCacheLatchImpl(manager.getCacheMetricsFactory(), EVCacheLatch.Policy.ALL, clients.length, app);
        for (EVCacheClient client : clients) {
            client.set(key, val, 60 * 60, latch);
        }
        boolean success = latch.await(1000, TimeUnit.MILLISECONDS);
        if (log.isDebugEnabled()) log.debug("SET LATCH : key : " + key + "; Finished in " + (System.currentTimeMillis() - start) + " msec");
        return success;
    }

    protected boolean deleteLatch(int i, String appName) throws Exception {
        long start = System.currentTimeMillis();
        String key = "key_" + i;
        final EVCacheClient[] clients = manager.getEVCacheClientPool(appName).getEVCacheClientForWrite();
        final EVCacheLatch latch = new EVCacheLatchImpl(manager.getCacheMetricsFactory(), Policy.ALL, clients.length, appName);
        for (EVCacheClient client : clients) {
            client.delete(key, latch);
        }
        latch.await(1000, TimeUnit.MILLISECONDS);
        if (log.isDebugEnabled()) log.debug("DELETE LATCH : key : " + key + "; Finished in " + (System.currentTimeMillis() - start) + " msec" + "; Latch : " + latch);
        return true;
    }

    public String get(int i, EVCache gCache) throws Exception {
        String key = "key_" + i;
        String value = gCache.<String>get(key);
        if(log.isDebugEnabled()) log.debug("get : key : " + key + " val = " + value);
        return value;
    }

    public String getAndTouch(int i, EVCache gCache) throws Exception {
        String key = "key_" + i;
        String value = gCache.<String>getAndTouch(key, 60 * 60);
        if(log.isDebugEnabled()) log.debug("getAndTouch : key : " + key + " val = " + value);
        return value;
    }

    public Map<String, String> getBulk(String keys[], EVCache gCache) throws Exception {
        final Map<String, String> value = gCache.<String>getBulk(keys);
        if(log.isDebugEnabled()) log.debug("getBulk : keys : " + Arrays.toString(keys) + "; values = " + value);
        return value;
    }

    public Map<String, String> getBulkAndTouch(String keys[], EVCache gCache, int ttl) throws Exception {
        final Map<String, String> value = gCache.<String>getBulkAndTouch(Arrays.asList(keys), null, ttl);
        if(log.isDebugEnabled()) log.debug("getBulk : keys : " + Arrays.toString(keys) + "; values = " + value);
        return value;
    }

    public String getObservable(int i, EVCache gCache, Scheduler scheduler) throws Exception {
        String key = "key_" + i;
        String value = gCache.<String>get(key, scheduler).toBlocking().value();
        if(log.isDebugEnabled()) log.debug("get : key : " + key + " val = " + value);
        return value;
    }

    public String getAndTouchObservable(int i, EVCache gCache, Scheduler scheduler) throws Exception {
        String key = "key_" + i;
        String value = gCache.<String>getAndTouch(key, 60 * 60, scheduler).toBlocking().value();
        if(log.isDebugEnabled()) log.debug("getAndTouch : key : " + key + " val = " + value);
        return value;
    }

    class RemoteCaller implements Runnable {
        EVCache gCache;
        public RemoteCaller(EVCache c) {
            this.gCache = c;
        }
        public void run() {
            try {
                int count = 1;
                for(int i = 0; i < 100; i++) {
                    insert(i, gCache);
                    get(i, gCache);
                    delete(i, gCache);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
    
    @Override
    public void run() {
        TestNG testng = new TestNG();
        testng.setTestClasses(new Class[] { getClass() });
        testng.run();
    }
}
