package com.netflix.evcache.pool;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ChainedDynamicProperty;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.discovery.shared.Pair;
import com.netflix.evcache.EVCacheConnectException;
import com.netflix.evcache.EVCacheException;
import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.EVCacheReadQueueException;
import com.netflix.evcache.EVCacheTranscoder;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.operation.EVCacheFutures;
import com.netflix.evcache.operation.EVCacheLatchImpl;
import com.netflix.evcache.pool.observer.EVCacheConnectionObserver;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.evcache.util.KeyHasher;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.tag.BasicTagList;
import com.netflix.servo.tag.TagList;

import net.spy.memcached.CASValue;
import net.spy.memcached.CachedData;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.EVCacheMemcachedClient;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.internal.ListenableFuture;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.protocol.binary.EVCacheNodeImpl;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import rx.Scheduler;
import rx.Single;

@SuppressWarnings({"rawtypes", "unchecked"})
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({ "REC_CATCH_EXCEPTION",
        "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE" })
public class EVCacheClient {

    private static Logger log = LoggerFactory.getLogger(EVCacheClient.class);
    private final ConnectionFactory connectionFactory;
    private final EVCacheMemcachedClient evcacheMemcachedClient;
    private final List<InetSocketAddress> memcachedNodesInZone;
    private EVCacheConnectionObserver connectionObserver = null;
    private boolean shutdown = false;

    private final int id;
    private final String appName;
    private final String zone;
    private final ServerGroup serverGroup;
    private final EVCacheServerGroupConfig config;
    private final int maxWriteQueueSize;

    private final ChainedDynamicProperty.IntProperty readTimeout;
    private final ChainedDynamicProperty.IntProperty bulkReadTimeout;
//    private final DynamicIntProperty operationTimeout;
    private final DynamicIntProperty maxReadQueueSize;
    private final DynamicBooleanProperty ignoreInactiveNodes;
    private final ChainedDynamicProperty.BooleanProperty enableChunking;
    private final DynamicBooleanProperty hashKeyByApp;
    private final DynamicBooleanProperty hashKeyByServerGroup;
    private final ChainedDynamicProperty.IntProperty chunkSize, writeBlock;
    private final ChunkTranscoder chunkingTranscoder;
    private final EVCacheTranscoder evcacheValueTranscoder;
    private final SerializingTranscoder decodingTranscoder;
    private static final int SPECIAL_BYTEARRAY = (8 << 8);
    private final EVCacheClientPool pool;
    private Counter addCounter = null;
    private final ChainedDynamicProperty.BooleanProperty ignoreTouch;
    protected final TagList tags;
    private final ChainedDynamicProperty.StringProperty hashingAlgo;

    EVCacheClient(String appName, String zone, int id, EVCacheServerGroupConfig config,
            List<InetSocketAddress> memcachedNodesInZone, int maxQueueSize, DynamicIntProperty maxReadQueueSize,
            ChainedDynamicProperty.IntProperty readTimeout, ChainedDynamicProperty.IntProperty bulkReadTimeout,
            DynamicIntProperty opQueueMaxBlockTime,
            DynamicIntProperty operationTimeout, EVCacheClientPool pool) throws IOException {
        this.memcachedNodesInZone = memcachedNodesInZone;
        this.id = id;
        this.appName = appName;
        this.zone = zone;
        this.config = config;
        this.serverGroup = config.getServerGroup();
        this.readTimeout = readTimeout;
        this.bulkReadTimeout = bulkReadTimeout;
        this.maxReadQueueSize = maxReadQueueSize;
//        this.operationTimeout = operationTimeout;
        this.pool = pool;
        this.connectionFactory = pool.getEVCacheClientPoolManager().getConnectionFactoryProvider().getConnectionFactory(appName, id, serverGroup, pool.getEVCacheClientPoolManager());
        this.enableChunking = EVCacheConfig.getInstance().getChainedBooleanProperty(this.serverGroup.getName()+ ".chunk.data", appName + ".chunk.data", Boolean.FALSE, null);
        this.chunkSize = EVCacheConfig.getInstance().getChainedIntProperty(this.serverGroup.getName() + ".chunk.size", appName + ".chunk.size", 1180, null);
        this.writeBlock = EVCacheConfig.getInstance().getChainedIntProperty(appName + "." + this.serverGroup.getName() + ".write.block.duration", appName + ".write.block.duration", 25, null);
        this.chunkingTranscoder = new ChunkTranscoder();
        this.maxWriteQueueSize = maxQueueSize;
        this.ignoreTouch = EVCacheConfig.getInstance().getChainedBooleanProperty(appName + "." + this.serverGroup.getName() + ".ignore.touch", appName + ".ignore.touch", false, null);
        this.ignoreInactiveNodes = EVCacheConfig.getInstance().getDynamicBooleanProperty(appName + ".ignore.inactive.nodes", true);

        this.evcacheMemcachedClient = new EVCacheMemcachedClient(connectionFactory, memcachedNodesInZone, readTimeout, appName, zone, id, serverGroup, this);
        this.connectionObserver = new EVCacheConnectionObserver(appName, serverGroup, id);
        this.evcacheMemcachedClient.addObserver(connectionObserver);
        this.tags = BasicTagList.of("ServerGroup", serverGroup.getName(), "APP", appName, "Id", String.valueOf(id));

        this.decodingTranscoder = new SerializingTranscoder(Integer.MAX_VALUE);
        decodingTranscoder.setCompressionThreshold(Integer.MAX_VALUE);
        
        this.evcacheValueTranscoder = new EVCacheTranscoder();
        evcacheValueTranscoder.setCompressionThreshold(Integer.MAX_VALUE);

        this.hashKeyByApp = EVCacheConfig.getInstance().getDynamicBooleanProperty(appName + ".hash.key", Boolean.FALSE);
        this.hashKeyByServerGroup = EVCacheConfig.getInstance().getDynamicBooleanProperty(this.serverGroup.getName() + ".hash.key", Boolean.FALSE);
        this.hashingAlgo = EVCacheConfig.getInstance().getChainedStringProperty(this.serverGroup.getName() + ".hash.algo", appName + ".hash.algo", "MD5", null);
    }

    private Collection<String> validateReadQueueSize(Collection<String> canonicalKeys) throws EVCacheException {
        if (evcacheMemcachedClient.getNodeLocator() == null) return canonicalKeys;
        final Collection<String> retKeys = new ArrayList<>(canonicalKeys.size());
        for (String key : canonicalKeys) {
            final MemcachedNode node = evcacheMemcachedClient.getNodeLocator().getPrimary(key);
            if (node instanceof EVCacheNodeImpl) {
                final EVCacheNodeImpl evcNode = (EVCacheNodeImpl) node;
                if (!evcNode.isAvailable()) {
                    continue;
                }

                final int size = evcNode.getReadQueueSize();
                final boolean canAddToOpQueue = size < (maxReadQueueSize.get() * 2);
                // if (log.isDebugEnabled()) log.debug("Bulk Current Read Queue
                // Size - " + size + " for app " + appName + " & zone " + zone +
                // " ; node " + node);
                if (!canAddToOpQueue) {
                    EVCacheMetricsFactory.getCounter(appName + "-READ_QUEUE_FULL", evcNode.getBaseTags()).increment();
                    if (log.isDebugEnabled()) log.debug("Read Queue Full on Bulk Operation for app : " + appName
                            + "; zone : " + zone + "; Current Size : " + size + "; Max Size : " + maxReadQueueSize.get() * 2);
                } else {
                    retKeys.add(key);
                }
            }
        }
        return retKeys;
    }

    private boolean ensureWriteQueueSize(MemcachedNode node, String key) throws EVCacheException {
        if (node instanceof EVCacheNodeImpl) {
            final EVCacheNodeImpl evcNode = (EVCacheNodeImpl) node;
            int i = 0;
            while (true) {
                final int size = evcNode.getWriteQueueSize();
                final boolean canAddToOpQueue = size < maxWriteQueueSize;
                if (log.isDebugEnabled()) log.debug("App : " + appName + "; zone : " + zone + "; key : " + key
                        + "; WriteQSize : " + size);
                if (canAddToOpQueue) break;
                EVCacheMetricsFactory.getCounter("EVCacheClient-" + appName + "-WRITE_BLOCK", evcNode.getBaseTags()).increment();
                try {
                    Thread.sleep(writeBlock.get());
                } catch (InterruptedException e) {
                    throw new EVCacheException("Thread was Interrupted", e);
                }

                if(i++ > 3) {
                    EVCacheMetricsFactory.getCounter("EVCacheClient-" + appName + "-INACTIVE_NODE", evcNode.getBaseTags()).increment();
                    if (log.isDebugEnabled()) log.debug("Node : " + evcNode + " for app : " + appName + "; zone : "
                            + zone + " is not active. Will Fail Fast and the write will be dropped for key : " + key);
                    evcNode.shutdown();
                    return false;
                }
            }
        }
        return true;
    }

    private boolean validateNode(String key, boolean _throwException) throws EVCacheException, EVCacheConnectException {
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        // First check if the node is active
        if (node instanceof EVCacheNodeImpl) {
            final EVCacheNodeImpl evcNode = (EVCacheNodeImpl) node;
            if (!evcNode.isAvailable()) {
            	EVCacheMetricsFactory.getCounter("EVCacheClient-" + appName + "-INACTIVE_NODE", evcNode.getBaseTags()).increment();
                if (log.isDebugEnabled()) log.debug("Node : " + node + " for app : " + appName + "; zone : " + zone
                        + " is not active. Will Fail Fast so that we can fallback to Other Zone if available.");
                if (_throwException) throw new EVCacheConnectException("Connection for Node : " + node + " for app : " + appName
                        + "; zone : " + zone + " is not active");
                return false;
            }

            final int size = evcNode.getReadQueueSize();
            final boolean canAddToOpQueue = size < maxReadQueueSize.get();
            if (log.isDebugEnabled()) log.debug("Current Read Queue Size - " + size + " for app " + appName + " & zone "
                    + zone + " and node : " + evcNode);
            if (!canAddToOpQueue) {
                EVCacheMetricsFactory.getCounter(appName + "-READ_QUEUE_FULL", evcNode.getBaseTags()).increment();
                if (log.isDebugEnabled()) log.debug("Read Queue Full for Node : " + node + "; app : " + appName
                        + "; zone : " + zone + "; Current Size : " + size + "; Max Size : " + maxReadQueueSize.get());
                if (_throwException) throw new EVCacheReadQueueException("Read Queue Full for Node : " + node + "; app : "
                        + appName + "; zone : " + zone + "; Current Size : " + size + "; Max Size : " + maxReadQueueSize.get());
                return false;
            }
        }
        return true;
    }

    private <T> ChunkDetails<T> getChunkDetails(String key) {
 
        final List<String> firstKeys = new ArrayList<String>(2);
        firstKeys.add(key);
        final String firstKey = key + "_00";
        firstKeys.add(firstKey);
        try {
            final Map<String, CachedData> metadataMap = evcacheMemcachedClient.asyncGetBulk(firstKeys, chunkingTranscoder, null, "GetChunkMetadataOperation")
                    .getSome(readTimeout.get(), TimeUnit.MILLISECONDS, false, false);
            if (metadataMap.containsKey(key)) {
                return new ChunkDetails(null, null, false, metadataMap.get(key));
            } else if (metadataMap.containsKey(firstKey)) {
                final ChunkInfo ci = getChunkInfo(firstKey, (String) decodingTranscoder.decode(metadataMap.get(
                        firstKey)));
                if (ci == null) return null;

                final List<String> keys = new ArrayList<>();
                for (int i = 1; i < ci.getChunks(); i++) {
                    final String prefix = (i < 10) ? "0" : "";
                    keys.add(ci.getKey() + "_" + prefix + i);
                }
                return new ChunkDetails(keys, ci, true, null);
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    private <T> Single<ChunkDetails<T>> getChunkDetails(String key, Scheduler scheduler) {

        final List<String> firstKeys = new ArrayList<>(2);
        firstKeys.add(key);
        final String firstKey = key + "_00";
        firstKeys.add(firstKey);

        return evcacheMemcachedClient.asyncGetBulk(firstKeys, chunkingTranscoder, null, "GetChunkMetadataOperation")
            .getSome(readTimeout.get(), TimeUnit.MILLISECONDS, false, false, scheduler)
            .map(metadataMap -> {
                if (metadataMap.containsKey(key)) {
                    return new ChunkDetails(null, null, false, metadataMap.get(key));
                } else if (metadataMap.containsKey(firstKey)) {
                    final ChunkInfo ci = getChunkInfo(firstKey, (String) decodingTranscoder.decode(metadataMap.get(
                        firstKey)));
                    if (ci == null) return null;

                    final List<String> keys = new ArrayList<>();
                    for (int i = 1; i < ci.getChunks(); i++) {
                        final String prefix = (i < 10) ? "0" : "";
                        keys.add(ci.getKey() + "_" + prefix + i);
                    }
                    return new ChunkDetails(keys, ci, true, null);
                } else {
                    return null;
                }
            });
    }

    private <T> T assembleChunks(String key, boolean touch, int ttl, Transcoder<T> tc, boolean hasZF) {
        final Stopwatch operationDuration = EVCacheMetricsFactory.getStatsTimer(appName, serverGroup, "LatencyChunk").start();
        try {
            
            final ChunkDetails<T> cd = getChunkDetails(key);
            if (cd == null) return null;
            if (!cd.isChunked()) {
                if (cd.getData() == null) return null;
                final Transcoder<T> transcoder = (tc == null ? (Transcoder<T>) evcacheMemcachedClient.getTranscoder()
                        : tc);
                return transcoder.decode((CachedData) cd.getData());
            } else {
                final List<String> keys = cd.getChunkKeys();
                final ChunkInfo ci = cd.getChunkInfo();

                final Map<String, CachedData> dataMap = evcacheMemcachedClient.asyncGetBulk(keys, chunkingTranscoder, null, "GetChunksOperation")
                        .getSome(readTimeout.get(), TimeUnit.MILLISECONDS, false, false);

                if (dataMap.size() != ci.getChunks() - 1) {
                    EVCacheMetricsFactory.increment(appName + "-INCORRECT_NUM_CHUNKS");
                    return null;
                }

                final byte[] data = new byte[(ci.getChunks() - 2) * ci.getChunkSize() + (ci.getLastChunk() == 0 ? ci
                        .getChunkSize() : ci.getLastChunk())];
                int index = 0;
                for (int i = 0; i < keys.size(); i++) {
                    final String _key = keys.get(i);
                    final CachedData _cd = dataMap.get(_key);
                    if (log.isDebugEnabled()) log.debug("Chunk Key " + _key + "; Value : " + _cd);
                    if (_cd == null) continue;

                    final byte[] val = _cd.getData();

                    // If we expect a chunk to be present and it is null then return null immediately.
                    if (val == null) return null; 
                    final int len = (i == keys.size() - 1) ? ((ci.getLastChunk() == 0 || ci.getLastChunk() > ci
                            .getChunkSize()) ? ci.getChunkSize() : ci.getLastChunk())
                            : val.length;
                    if (len != ci.getChunkSize() && i != keys.size() - 1) {
                        EVCacheMetricsFactory.increment(appName + "-INVALID_CHUNK_SIZE");
                        if (log.isWarnEnabled()) log.warn("CHUNK_SIZE_ERROR : Chunks : " + ci.getChunks() + " ; "
                                + "length : " + len + "; expectedLength : " + ci.getChunkSize() + " for key : " + _key);
                    }
                    if (len > 0) {
                        try {
                            System.arraycopy(val, 0, data, index, len);
                        } catch (Exception e) {
                            StringBuilder sb = new StringBuilder();
                            sb.append("ArrayCopyError - Key : " + _key + "; final data Size : " + data.length
                                    + "; copy array size : " + len + "; val size : " + val.length
                                    + "; key index : " + i + "; copy from : " + index + "; ChunkInfo : " + ci + "\n");
                            for (int j = 0; j < keys.size(); j++) {
                                final String skey = keys.get(j);
                                final byte[] sval = (byte[]) dataMap.get(skey).getData();
                                sb.append(skey + "=" + sval.length + "\n");
                            }
                            if (log.isWarnEnabled()) log.warn(sb.toString(), e);
                            throw e;
                        }

                        index += val.length;
                        if (touch) evcacheMemcachedClient.touch(_key, ttl);
                    }
                }

                final boolean checksumPass = checkCRCChecksum(data, ci, hasZF);
                if (!checksumPass) return null;
                final Transcoder<T> transcoder = (tc == null ? (Transcoder<T>) evcacheMemcachedClient.getTranscoder()
                        : tc);
                return transcoder.decode(new CachedData(ci.getFlags(), data, Integer.MAX_VALUE));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if(operationDuration != null) operationDuration.stop();
        }
        return null;
    }

    private <T> Single<T> assembleChunks(String key, boolean touch, int ttl, Transcoder<T> tc, boolean hasZF, Scheduler scheduler) {
        final Stopwatch operationDuration = EVCacheMetricsFactory.getStatsTimer(appName, serverGroup, "LatencyChunk").start();
        return getChunkDetails(key, scheduler).flatMap(cd -> {
            if (cd == null) return Single.just(null);
            if (!cd.isChunked()) {
                if (cd.getData() == null) return Single.just(null);
                final Transcoder<T> transcoder = (tc == null ? (Transcoder<T>) evcacheMemcachedClient.getTranscoder() : tc);
                return Single.just(transcoder.decode((CachedData) cd.getData()));
            } else {
                final List<String> keys = cd.getChunkKeys();
                final ChunkInfo ci = cd.getChunkInfo();

                return evcacheMemcachedClient.asyncGetBulk(keys, chunkingTranscoder, null, "GetChunksOperation")
                    .getSome(readTimeout.get(), TimeUnit.MILLISECONDS, false, false, scheduler)
                    .map(dataMap -> {
                        if (dataMap.size() != ci.getChunks() - 1) {
                            EVCacheMetricsFactory.increment(appName + "-INCORRECT_NUM_CHUNKS");
                            return null;
                        }

                        final byte[] data = new byte[(ci.getChunks() - 2) * ci.getChunkSize() + (ci.getLastChunk() == 0 ? ci
                            .getChunkSize() : ci.getLastChunk())];
                        int index = 0;
                        for (int i = 0; i < keys.size(); i++) {
                            final String _key = keys.get(i);
                            final CachedData _cd = dataMap.get(_key);
                            if (log.isDebugEnabled()) log.debug("Chunk Key " + _key + "; Value : " + _cd);
                            if (_cd == null) continue;

                            final byte[] val = _cd.getData();

                            // If we expect a chunk to be present and it is null then return null immediately.
                            if (val == null) return null;
                            final int len = (i == keys.size() - 1) ? ((ci.getLastChunk() == 0 || ci.getLastChunk() > ci
                                .getChunkSize()) ? ci.getChunkSize() : ci.getLastChunk())
                                : val.length;
                            if (len != ci.getChunkSize() && i != keys.size() - 1) {
                                EVCacheMetricsFactory.increment(appName + "-INVALID_CHUNK_SIZE");
                                if (log.isWarnEnabled()) log.warn("CHUNK_SIZE_ERROR : Chunks : " + ci.getChunks() + " ; "
                                    + "length : " + len + "; expectedLength : " + ci.getChunkSize() + " for key : " + _key);
                            }
                            if (len > 0) {
                                try {
                                    System.arraycopy(val, 0, data, index, len);
                                } catch (Exception e) {
                                    StringBuilder sb = new StringBuilder();
                                    sb.append("ArrayCopyError - Key : " + _key + "; final data Size : " + data.length
                                        + "; copy array size : " + len + "; val size : " + val.length
                                        + "; key index : " + i + "; copy from : " + index + "; ChunkInfo : " + ci + "\n");
                                    for (int j = 0; j < keys.size(); j++) {
                                        final String skey = keys.get(j);
                                        final byte[] sval = (byte[]) dataMap.get(skey).getData();
                                        sb.append(skey + "=" + sval.length + "\n");
                                    }
                                    if (log.isWarnEnabled()) log.warn(sb.toString(), e);
                                    throw e;
                                }

                                System.arraycopy(val, 0, data, index, len);
                                index += val.length;
                                if (touch) evcacheMemcachedClient.touch(_key, ttl);
                            }
                        }

                        final boolean checksumPass = checkCRCChecksum(data, ci, hasZF);
                        if (!checksumPass) return null;
                        final Transcoder<T> transcoder = (tc == null ? (Transcoder<T>) evcacheMemcachedClient.getTranscoder()
                            : tc);
                        return transcoder.decode(new CachedData(ci.getFlags(), data, Integer.MAX_VALUE));
                    });
            }
        }).doAfterTerminate(() ->
            operationDuration.stop()
        );
    }

    private boolean checkCRCChecksum(byte[] data, final ChunkInfo ci, boolean hasZF) {
        if (data == null || data.length == 0) return false;

        final Checksum checksum = new CRC32();
        checksum.update(data, 0, data.length);
        final long currentChecksum = checksum.getValue();
        final long expectedChecksum = ci.getChecksum();
        if (log.isDebugEnabled()) log.debug("CurrentChecksum : " + currentChecksum + "; ExpectedChecksum : "
                + expectedChecksum + " for key : " + ci.getKey());
        if (currentChecksum != expectedChecksum) {
            if (!hasZF) {
                if (log.isWarnEnabled()) log.warn("CHECKSUM_ERROR : Chunks : " + ci.getChunks() + " ; "
                        + "currentChecksum : " + currentChecksum + "; expectedChecksum : " + expectedChecksum
                        + " for key : " + ci.getKey());
                EVCacheMetricsFactory.increment(appName + "-CHECK_SUM_ERROR");
            }
            return false;
        }
        return true;
    }

    private ChunkInfo getChunkInfo(String firstKey, String metadata) {
        if (metadata == null) return null;
        final String[] metaItems = metadata.split(":");
        if (metaItems.length != 5) return null;
        final String key = firstKey.substring(0, firstKey.length() - 3);

        final ChunkInfo ci = new ChunkInfo(Integer.parseInt(metaItems[0]), Integer.parseInt(metaItems[1]), Integer
                .parseInt(metaItems[2]), Integer.parseInt(metaItems[3]), key, Long
                        .parseLong(metaItems[4]));
        return ci;
    }

    private <T> Map<String, T> assembleChunks(Collection<String> keyList, Transcoder<T> tc, boolean hasZF) {
        final List<String> firstKeys = new ArrayList<>();
        for (String key : keyList) {
            firstKeys.add(key);
            firstKeys.add(key + "_00");
        }
        final Stopwatch operationDuration = EVCacheMetricsFactory.getStatsTimer(appName, serverGroup, "LatencyChunk").start();
        try {
            final Map<String, CachedData> metadataMap = evcacheMemcachedClient.asyncGetBulk(firstKeys, chunkingTranscoder, null, "GetChunkMetadataOperation")
                    .getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, false, false);
            if (metadataMap == null) return null;

            final Map<String, T> returnMap = new HashMap<>(keyList.size() * 2);
            for (String key : keyList) {
                if (metadataMap.containsKey(key)) {
                    CachedData val = metadataMap.remove(key);
                    returnMap.put(key, tc.decode(val));
                }
            }

            final List<String> allKeys = new ArrayList<>();
            final Map<ChunkInfo, Pair<List<String>, byte[]>> responseMap = new HashMap<>();
            for (Entry<String, CachedData> entry : metadataMap.entrySet()) {
                final String firstKey = entry.getKey();
                final String metadata = (String) decodingTranscoder.decode(entry.getValue());
                if (metadata == null) continue;
                final ChunkInfo ci = getChunkInfo(firstKey, metadata);
                if (ci != null) {
                    final List<String> ciKeys = new ArrayList<>();

                    for (int i = 1; i < ci.getChunks(); i++) {
                        final String prefix = (i < 10) ? "0" : "";
                        final String _key = ci.getKey() + "_" + prefix + i;
                        allKeys.add(_key);
                        ciKeys.add(_key);
                    }

                    final byte[] data = new byte[(ci.getChunks() - 2) * ci.getChunkSize() + ci.getLastChunk()];
                    responseMap.put(ci, new Pair<>(ciKeys, data));
                }
            }

            final Map<String, CachedData> dataMap = evcacheMemcachedClient.asyncGetBulk(allKeys, chunkingTranscoder, null, "GetChunksOperation")
                    .getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, false, false);

            for (Entry<ChunkInfo, Pair<List<String>, byte[]>> entry : responseMap.entrySet()) {
                final ChunkInfo ci = entry.getKey();
                final Pair<List<String>, byte[]> pair = entry.getValue();
                final List<String> ciKeys = pair.first();
                byte[] data = pair.second();
                int index = 0;
                for (int i = 0; i < ciKeys.size(); i++) {
                    final String _key = ciKeys.get(i);
                    final CachedData cd = dataMap.get(_key);
                    if (log.isDebugEnabled()) log.debug("Chunk Key " + _key + "; Value : " + cd);
                    if (cd == null) continue;
                    final byte[] val = cd.getData();

                    if (val == null) {
                        data = null;
                        break;
                    }
                    final int len = (i == ciKeys.size() - 1) ? ((ci.getLastChunk() == 0 || ci.getLastChunk() > ci
                            .getChunkSize()) ? ci.getChunkSize() : ci.getLastChunk())
                            : val.length;
                    try {
                        System.arraycopy(val, 0, data, index, len);
                    } catch (Exception e) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("ArrayCopyError - Key : " + _key + "; final data Size : " + data.length
                                + "; copy array size : " + len + "; val size : " + val.length
                                + "; key index : " + i + "; copy from : " + index + "; ChunkInfo : " + ci + "\n");
                        for (int j = 0; j < ciKeys.size(); j++) {
                            final String skey = ciKeys.get(j);
                            final byte[] sval = dataMap.get(skey).getData();
                            sb.append(skey + "=" + sval.length + "\n");
                        }
                        if (log.isWarnEnabled()) log.warn(sb.toString(), e);
                        throw e;
                    }
                    index += val.length;
                }
                final boolean checksumPass = checkCRCChecksum(data, ci, hasZF);
                if (data != null && checksumPass) {
                    final CachedData cd = new CachedData(ci.getFlags(), data, Integer.MAX_VALUE);
                    returnMap.put(ci.getKey(), tc.decode(cd));
                } else {
                    returnMap.put(ci.getKey(), null);
                }
            }
            return returnMap;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if(operationDuration != null) operationDuration.stop();
        }
        return null;
    }

    private <T> Single<Map<String, T>> assembleChunks(Collection<String> keyList, Transcoder<T> tc, boolean hasZF, Scheduler scheduler) {
        final List<String> firstKeys = new ArrayList<>();
        for (String key : keyList) {
            firstKeys.add(key);
            firstKeys.add(key + "_00");
        }
        final Stopwatch operationDuration = EVCacheMetricsFactory.getStatsTimer(appName, serverGroup, "LatencyChunk").start();

        return evcacheMemcachedClient.asyncGetBulk(firstKeys, chunkingTranscoder, null, "GetChunkMetadataOperation")
            .getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, false, false, scheduler)
            .flatMap(metadataMap -> {
                if (metadataMap == null) return null;

                final Map<String, T> returnMap = new HashMap<>(keyList.size() * 2);
                for (String key : keyList) {
                    if (metadataMap.containsKey(key)) {
                        CachedData val = metadataMap.remove(key);
                        returnMap.put(key, tc.decode(val));
                    }
                }

                final List<String> allKeys = new ArrayList<>();
                final Map<ChunkInfo, Pair<List<String>, byte[]>> responseMap = new HashMap<>();
                for (Entry<String, CachedData> entry : metadataMap.entrySet()) {
                    final String firstKey = entry.getKey();
                    final String metadata = (String) decodingTranscoder.decode(entry.getValue());
                    if (metadata == null) continue;
                    final ChunkInfo ci = getChunkInfo(firstKey, metadata);
                    if (ci != null) {
                        final List<String> ciKeys = new ArrayList<>();

                        for (int i = 1; i < ci.getChunks(); i++) {
                            final String prefix = (i < 10) ? "0" : "";
                            final String _key = ci.getKey() + "_" + prefix + i;
                            allKeys.add(_key);
                            ciKeys.add(_key);
                        }

                        final byte[] data = new byte[(ci.getChunks() - 2) * ci.getChunkSize() + ci.getLastChunk()];
                        responseMap.put(ci, new Pair<>(ciKeys, data));
                    }
                }

                return evcacheMemcachedClient.asyncGetBulk(allKeys, chunkingTranscoder, null, "GetChunksOperation")
                    .getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, false, false, scheduler)
                    .map(dataMap -> {
                        for (Entry<ChunkInfo, Pair<List<String>, byte[]>> entry : responseMap.entrySet()) {
                            final ChunkInfo ci = entry.getKey();
                            final Pair<List<String>, byte[]> pair = entry.getValue();
                            final List<String> ciKeys = pair.first();
                            byte[] data = pair.second();
                            int index = 0;
                            for (int i = 0; i < ciKeys.size(); i++) {
                                final String _key = ciKeys.get(i);
                                final CachedData cd = dataMap.get(_key);
                                if (log.isDebugEnabled()) log.debug("Chunk Key " + _key + "; Value : " + cd);
                                if (cd == null) continue;
                                final byte[] val = cd.getData();

                                if (val == null) {
                                    data = null;
                                    break;
                                }
                                final int len = (i == ciKeys.size() - 1) ? ((ci.getLastChunk() == 0 || ci.getLastChunk() > ci
                                    .getChunkSize()) ? ci.getChunkSize() : ci.getLastChunk())
                                    : val.length;
                                try {
                                    System.arraycopy(val, 0, data, index, len);
                                } catch (Exception e) {
                                    StringBuilder sb = new StringBuilder();
                                    sb.append("ArrayCopyError - Key : " + _key + "; final data Size : " + data.length
                                        + "; copy array size : " + len + "; val size : " + val.length
                                        + "; key index : " + i + "; copy from : " + index + "; ChunkInfo : " + ci + "\n");
                                    for (int j = 0; j < ciKeys.size(); j++) {
                                        final String skey = ciKeys.get(j);
                                        final byte[] sval = dataMap.get(skey).getData();
                                        sb.append(skey + "=" + sval.length + "\n");
                                    }
                                    if (log.isWarnEnabled()) log.warn(sb.toString(), e);
                                    throw e;
                                }
                                index += val.length;
                            }
                            final boolean checksumPass = checkCRCChecksum(data, ci, hasZF);
                            if (data != null && checksumPass) {
                                final CachedData cd = new CachedData(ci.getFlags(), data, Integer.MAX_VALUE);
                                returnMap.put(ci.getKey(), tc.decode(cd));
                            } else {
                                returnMap.put(ci.getKey(), null);
                            }
                        }

                        return returnMap;
                    });
            }).doAfterTerminate(() ->
                operationDuration.stop()
            );
    }

    private CachedData[] createChunks(CachedData cd, String key) {
        final int cSize = chunkSize.get();
        if ((key.length() + 3) > cSize) throw new IllegalArgumentException("The chunksize " + cSize
                + " is smaller than the key size. Will not be able to proceed. key size = "
                + key.length());
        final int len = cd.getData().length;

        /* the format of headers in memcached */
        // Key size + 1 + Header( Flags (Characters Number) + Key (Characters Numbers) + 2 bytes ( \r\n ) + 4 bytes (2 spaces and 1 \r)) + Chunk Size + CAS Size
        // final int overheadSize = key.length() // Key Size
        // + 1 // Space
        // + 4 // Flags (Characters Number)
        // + 4 // Key (Characters Numbers)
        // + 2 // /r/n
        // + 4 // 2 spaces and 1 \r
        // + 48 // Header Size
        // + 8; // CAS
        final int overheadSize = key.length() + 71 + 3; 
        // 3 because we will suffix _00, _01 ... _99; 68 is the size of the memcached header
        final int actualChunkSize = cSize - overheadSize;
        int lastChunkSize = len % actualChunkSize;
        final int numOfChunks = len / actualChunkSize + ((lastChunkSize > 0) ? 1 : 0) + 1;
        final CachedData[] chunkData = new CachedData[numOfChunks];
        if (lastChunkSize == 0) lastChunkSize = actualChunkSize;

        final long sTime = System.nanoTime();
        final Checksum checksum = new CRC32();
        checksum.update(cd.getData(), 0, len);
        final long checkSumValue = checksum.getValue();

        int srcPos = 0;
        if (log.isDebugEnabled()) log.debug("Ths size of data is " + len + " ; we will create " + (numOfChunks - 1)
                + " of " + actualChunkSize + " bytes. Checksum : "
                + checkSumValue + "; Checksum Duration : " + (System.nanoTime() - sTime));
        chunkData[0] = decodingTranscoder.encode(numOfChunks + ":" + actualChunkSize + ":" + lastChunkSize + ":" + cd
                .getFlags() + ":" + checkSumValue);
        for (int i = 1; i < numOfChunks; i++) {
            int lengthOfArray = actualChunkSize;
            if (srcPos + actualChunkSize > len) {
                lengthOfArray = len - srcPos;
            }
            byte[] dest = new byte[actualChunkSize];
            System.arraycopy(cd.getData(), srcPos, dest, 0, lengthOfArray);
            if (actualChunkSize > lengthOfArray) {
                for (int j = lengthOfArray; j < actualChunkSize; j++) {
                    dest[j] = Character.UNASSIGNED;// Adding filler data
                }
            }
            srcPos += lengthOfArray;
            //chunkData[i] = decodingTranscoder.encode(dest);
            chunkData[i] = new CachedData(SPECIAL_BYTEARRAY, dest, Integer.MAX_VALUE);
        }
        EVCacheMetricsFactory.getDistributionSummary(appName + "-ChunkData-NumberOfChunks", appName, serverGroup.getName()).record(numOfChunks);
        EVCacheMetricsFactory.getDistributionSummary(appName + "-ChunkData-TotalSize", appName, serverGroup.getName()).record(len);

        return chunkData;
    }

    /** 
     * Retrieves all the chunks as is. This is mainly used for debugging. 
     * 
     * @param key
     * @return Returns all the chunks retrieved. 
     * @throws EVCacheReadQueueException
     * @throws EVCacheException
     * @throws Exception
     */
    public Map<String, CachedData> getAllChunks(String key) throws EVCacheReadQueueException, EVCacheException, Exception {
        try {
            final ChunkDetails<Object> cd = getChunkDetails(key);
            if(log.isDebugEnabled()) log.debug("Chunkdetails " + cd);
            if (cd == null) return null;
            if (!cd.isChunked()) {
                Map<String, CachedData> rv = new HashMap<String, CachedData>();
                rv.put(key, (CachedData) cd.getData());
                if(log.isDebugEnabled()) log.debug("Data : " + rv);
                return rv;
            } else {
                final List<String> keys = cd.getChunkKeys();
                if(log.isDebugEnabled()) log.debug("Keys - " + keys);
                final Map<String, CachedData> dataMap = evcacheMemcachedClient.asyncGetBulk(keys, chunkingTranscoder, null, "GetAllChunksOperation")
                        .getSome(readTimeout.get().intValue(), TimeUnit.MILLISECONDS, false, false);
                
                if(log.isDebugEnabled()) log.debug("Datamap " + dataMap);
                return dataMap;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    public long incr(String key, long by, long defaultVal, int timeToLive) throws EVCacheException {
        return evcacheMemcachedClient.incr(key, by, defaultVal, timeToLive);
    }

    public long decr(String key, long by, long defaultVal, int timeToLive) throws EVCacheException {
        return evcacheMemcachedClient.decr(key, by, defaultVal, timeToLive);
    }

    public <T> T get(String key, Transcoder<T> tc, boolean _throwException, boolean hasZF, boolean chunked) throws Exception {
        if (chunked) {
            return assembleChunks(key, false, 0, tc, hasZF);
        } else if(shouldHashKey()) {
            final String hKey = getHashedKey(key);
            final Object obj = evcacheMemcachedClient.asyncGet(hKey, evcacheValueTranscoder, null).get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF);
            if(obj instanceof EVCacheValue) {
                final EVCacheValue val = (EVCacheValue)obj;
                if(val == null || !(val.getKey().equals(key))) {
                    EVCacheMetricsFactory.increment(appName, null, serverGroup.getName(), appName + "-KEY_HASH_COLLISION");
                    return null;
                }
                final CachedData cd = new CachedData(val.getFlags(), val.getValue(), CachedData.MAX_SIZE);
                if(tc == null) {
                    return (T)evcacheMemcachedClient.getTranscoder().decode(cd);
                } else {
                    return tc.decode(cd);
                }
            } else {
                return null;
            }
        } else {
            return evcacheMemcachedClient.asyncGet(key, tc, null).get(readTimeout.get(),
                    TimeUnit.MILLISECONDS, _throwException, hasZF);
        }
    }

    public <T> T get(String key, Transcoder<T> tc, boolean _throwException, boolean hasZF) throws Exception {
        if (!validateNode(key, _throwException)) {
            if(ignoreInactiveNodes.get()) {
                EVCacheMetricsFactory.increment(appName, null, serverGroup.getName(), appName + "-IGNORE_INACTIVE_NODES");
                return pool.getEVCacheClientForReadExclude(serverGroup).get(key, tc, _throwException, hasZF, enableChunking.get());
            } else {
                return null;
            }
        }
        return get(key, tc, _throwException, hasZF, enableChunking.get());
    }

    public <T> Single<T> get(String key, Transcoder<T> tc, boolean _throwException, boolean hasZF, boolean chunked, Scheduler scheduler)  throws Exception {
        if (chunked) {
            return assembleChunks(key, _throwException, 0, tc, hasZF, scheduler);
        }  else if(shouldHashKey()) {
            final String hKey = getHashedKey(key);
            final Object obj = evcacheMemcachedClient.asyncGet(hKey, evcacheValueTranscoder, null).get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF);
            if(obj instanceof EVCacheValue) {
                final EVCacheValue val = (EVCacheValue)obj;
                if(val == null || !(val.getKey().equals(key))) {
                    EVCacheMetricsFactory.increment(appName, null, serverGroup.getName(), appName + "-KEY_HASH_COLLISION");
                    return null;
                }
                final CachedData cd = new CachedData(val.getFlags(), val.getValue(), CachedData.MAX_SIZE);
                if(tc == null) {
                    return Single.just((T)evcacheMemcachedClient.getTranscoder().decode(cd));
                } else {
                    return Single.just(tc.decode(cd));
                }
            } else {
                return null;
            }
        } else {
            return evcacheMemcachedClient.asyncGet(key, tc, null)
                .get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF, scheduler);
        }
    }

    public <T> Single<T> get(String key, Transcoder<T> tc, boolean _throwException, boolean hasZF, Scheduler scheduler) {
        try {
            if (!validateNode(key, _throwException)) {
                if(ignoreInactiveNodes.get()) {
                    EVCacheMetricsFactory.increment(appName, null, serverGroup.getName(), appName + "-IGNORE_INACTIVE_NODES");
                    return pool.getEVCacheClientForReadExclude(serverGroup).get(key, tc, _throwException, hasZF, enableChunking.get(), scheduler);
                } else {
                    return Single.just(null);
                }
            }
            return get(key, tc, _throwException, hasZF, enableChunking.get(), scheduler);
        } catch (Throwable e) {
            return Single.error(e);
        }
    }

    public <T> T getAndTouch(String key, Transcoder<T> tc, int timeToLive, boolean _throwException, boolean hasZF) throws Exception {
        EVCacheMemcachedClient _client = evcacheMemcachedClient;
        if (!validateNode(key, _throwException)) {
            if(ignoreInactiveNodes.get()) {
                EVCacheMetricsFactory.increment(appName, null, serverGroup.getName(), appName + "-IGNORE_INACTIVE_NODES");
                _client = pool.getEVCacheClientForReadExclude(serverGroup).getEVCacheMemcachedClient();
            } else {
                return null;
            }
        }

        if (tc == null) tc = (Transcoder<T>) getTranscoder();
        final T returnVal;
        if (enableChunking.get()) {
            return assembleChunks(key, false, 0, tc, hasZF);
        } else if(shouldHashKey()) {
            final String hKey = getHashedKey(key);
            final Object obj;
            if(ignoreTouch.get()) {
                obj = _client.asyncGet(hKey, evcacheValueTranscoder, null).get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF);
            } else {
                final CASValue<Object> value = _client.asyncGetAndTouch(key, timeToLive, evcacheValueTranscoder).get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF);
                obj = (value == null) ? null : value.getValue();
            }
            if(obj != null && obj instanceof EVCacheValue) {
                final EVCacheValue val = (EVCacheValue)obj;
                if(val == null || !(val.getKey().equals(key))) {
                    EVCacheMetricsFactory.increment(appName, null, serverGroup.getName(), appName + "-KEY_HASH_COLLISION");
                    return null;
                }
                final CachedData cd = new CachedData(val.getFlags(), val.getValue(), CachedData.MAX_SIZE);
                if(tc == null) {
                    return (T)_client.getTranscoder().decode(cd);
                } else {
                    return tc.decode(cd);
                }
            } else {
                return null;
            }
        } else {
            if(ignoreTouch.get()) {
                returnVal = _client.asyncGet(key, tc, null).get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF);
            } else {
                final CASValue<T> value = _client.asyncGetAndTouch(key, timeToLive, tc).get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF);
                returnVal = (value == null) ? null : value.getValue();
            }
        }
        return returnVal;
    }

    public <T> Single<T> getAndTouch(String key, Transcoder<T> transcoder, int timeToLive, boolean _throwException, boolean hasZF, Scheduler scheduler) {
        try {
            EVCacheMemcachedClient client = evcacheMemcachedClient;
            if (!validateNode(key, _throwException)) {
                if(ignoreInactiveNodes.get()) {
                    EVCacheMetricsFactory.increment(appName, null, serverGroup.getName(), appName + "-IGNORE_INACTIVE_NODES");
                    client = pool.getEVCacheClientForReadExclude(serverGroup).getEVCacheMemcachedClient();
                } else {
                    return null;
                }
            }

            final EVCacheMemcachedClient _client = client; 
            final Transcoder<T> tc = (transcoder == null) ? (Transcoder<T>) getTranscoder(): transcoder;
            if (enableChunking.get()) {
                return assembleChunks(key, false, 0, tc, hasZF, scheduler);
            } else if(shouldHashKey()) {
                final String hKey = getHashedKey(key);
                if(ignoreTouch.get()) {
                    final Single<Object> value = _client.asyncGet(hKey, evcacheValueTranscoder, null).get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF, scheduler);
                    return value.flatMap(r -> {
                        final CASValue<Object> rObj = (CASValue<Object>)r;
                        final EVCacheValue val = (EVCacheValue)rObj.getValue();
                        if(val == null || !(val.getKey().equals(key))) {
                            EVCacheMetricsFactory.increment(appName, null, serverGroup.getName(), appName + "-KEY_HASH_COLLISION");
                            return null;
                        }
                        final CachedData cd = new CachedData(val.getFlags(), val.getValue(), CachedData.MAX_SIZE);
                        if(tc == null) {
                            return Single.just((T)_client.getTranscoder().decode(cd));
                        } else {
                            return Single.just(tc.decode(cd));
                        }
                    });                    
                } else {
                    final Single<CASValue<Object>> value = _client.asyncGetAndTouch(hKey, timeToLive, evcacheValueTranscoder).get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF, scheduler);
                    if(value != null ) {
                        return value.flatMap(r -> {
                            final CASValue<Object> rObj = (CASValue<Object>)r;
                            final EVCacheValue val = (EVCacheValue)rObj.getValue();
                            if(val == null || !(val.getKey().equals(key))) {
                                EVCacheMetricsFactory.increment(appName, null, serverGroup.getName(), appName + "-KEY_HASH_COLLISION");
                                return null;
                            }
                            final CachedData cd = new CachedData(val.getFlags(), val.getValue(), CachedData.MAX_SIZE);
                            if(tc == null) {
                                return Single.just((T)_client.getTranscoder().decode(cd));
                            } else {
                                return Single.just(tc.decode(cd));
                            }
                        });
                    } else {
                        return null;
                    }
                }
            } else {
                return _client.asyncGetAndTouch(key, timeToLive, tc)
                    .get(readTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF, scheduler)
                    .map(value -> (value == null) ? null : value.getValue());
            }
        } catch (Throwable e) {
            return Single.error(e);
        }
    }

    public <T> Map<String, T> getBulk(Collection<String> _canonicalKeys, Transcoder<T> tc, boolean _throwException,
            boolean hasZF) throws Exception {
        final Collection<String> canonicalKeys = validateReadQueueSize(_canonicalKeys);
        final Map<String, T> returnVal;
        try {
            if (tc == null) tc = (Transcoder<T>) getTranscoder();
            if (enableChunking.get()) {
                returnVal = assembleChunks(_canonicalKeys, tc, hasZF);
            } else if(shouldHashKey()) {
                final Collection<String> hashKeys = new ArrayList<String>(canonicalKeys.size());
                for(String cKey : canonicalKeys) {
                    final String hKey = getHashedKey(cKey);
                    hashKeys.add(hKey);
                }
                final Map<String, Object> vals = evcacheMemcachedClient.asyncGetBulk(hashKeys, evcacheValueTranscoder, null, "BulkOperation").getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF);
                if(vals != null && !vals.isEmpty()) {
                    returnVal = new HashMap<String, T>(vals.size());
                    for(Entry<String, Object> entry : vals.entrySet()) {
                        final Object obj = entry.getValue();
                        if(obj instanceof EVCacheValue) {
                            final EVCacheValue val = (EVCacheValue)obj;
                            final CachedData cd = new CachedData(val.getFlags(), val.getValue(), CachedData.MAX_SIZE);
                            if(tc == null) {
                                returnVal.put(val.getKey(), (T)evcacheMemcachedClient.getTranscoder().decode(cd));
                            } else {
                                returnVal.put(val.getKey(), tc.decode(cd));
                            }
                        } else {
                            if (log.isDebugEnabled()) log.debug("Value for key : " + entry.getKey() + " is not EVCacheValue. val : " + obj);
                        }
                    }
                } else {
                    return Collections.<String, T> emptyMap();
                }
            } else {
                returnVal = evcacheMemcachedClient.asyncGetBulk(canonicalKeys, tc, null, "BulkOperation")
                        .getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF);
            }
        } catch (Exception e) {
            if (_throwException) throw e;
            return Collections.<String, T> emptyMap();
        }
        return returnVal;
    }

    public <T> Single<Map<String, T>> getBulk(Collection<String> _canonicalKeys, final Transcoder<T> transcoder, boolean _throwException,
            boolean hasZF, Scheduler scheduler) {
        try {
            final Collection<String> canonicalKeys = validateReadQueueSize(_canonicalKeys);
            final Transcoder<T> tc = (transcoder == null) ? (Transcoder<T>) getTranscoder() : transcoder;
            if (enableChunking.get()) {
                return assembleChunks(_canonicalKeys, tc, hasZF, scheduler);
            } else if(shouldHashKey()) {
                final Collection<String> hashKeys = new ArrayList<String>(canonicalKeys.size());
                for(String cKey : canonicalKeys) {
                    final String hKey = getHashedKey(cKey);
                    hashKeys.add(hKey);
                }
                final Single<Map<String, Object>> vals = evcacheMemcachedClient.asyncGetBulk(hashKeys, evcacheValueTranscoder, null, "BulkOperation").getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF, scheduler);
                if(vals != null ) {
                    return vals.flatMap(r -> {
                        HashMap<String, T> returnVal = new HashMap<String, T>();
                        for(Entry<String, Object> entry : r.entrySet()) {
                            final Object obj = entry.getValue();
                            if(obj instanceof EVCacheValue) {
                                final EVCacheValue val = (EVCacheValue)obj;
                                final CachedData cd = new CachedData(val.getFlags(), val.getValue(), CachedData.MAX_SIZE);
                                if(tc == null) {
                                    returnVal.put(val.getKey(), (T)evcacheMemcachedClient.getTranscoder().decode(cd));
                                } else {
                                    returnVal.put(val.getKey(), tc.decode(cd));
                                }
                            } else {
                                if (log.isDebugEnabled()) log.debug("Value for key : " + entry.getKey() + " is not EVCacheValue. val : " + obj);
                            }
                        }
                        return Single.just(returnVal);
                    });
                } else {
                    return Single.just(Collections.<String, T> emptyMap());
                }
            } else {
                return evcacheMemcachedClient.asyncGetBulk(canonicalKeys, tc, null, "BulkOperation")
                    .getSome(bulkReadTimeout.get(), TimeUnit.MILLISECONDS, _throwException, hasZF, scheduler);
            }
        } catch (Throwable e) {
            return Single.error(e);
        }
    }

    public <T> Future<Boolean> append(String key, T value) throws Exception {
        if (enableChunking.get()) throw new EVCacheException(
                "This operation is not supported as chunking is enabled on this EVCacheClient.");
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key)) return getDefaultFuture();
        if(shouldHashKey()) {
            final String hKey = getHashedKey(key);
            return evcacheMemcachedClient.append(hKey, value);
        } else {
            return evcacheMemcachedClient.append(key, value);
        }
    }

    public Future<Boolean> set(String key, CachedData value, int timeToLive) throws Exception {
        return _set(key, value, timeToLive, null);
    }

    public Future<Boolean> set(String key, CachedData cd, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        return _set(key, cd, timeToLive, evcacheLatch);
    }

    @Deprecated
    public <T> Future<Boolean> set(String key, T value, int timeToLive) throws Exception {
        return set(key, value, timeToLive, null);
    }

    @Deprecated
    public <T> Future<Boolean> set(String key, T value, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        final CachedData cd;
        if (value instanceof CachedData) {
            cd = (CachedData) value;
        } else {
            cd = getTranscoder().encode(value);
        }
        return _set(key, cd, timeToLive, evcacheLatch);
    }

    private Future<Boolean> _set(String key, CachedData value, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key)) {
            if (log.isInfoEnabled()) log.info("Node : " + node + " is not active. Failing fast and dropping the write event.");
            final ListenableFuture<Boolean, OperationCompletionListener> defaultFuture = (ListenableFuture<Boolean, OperationCompletionListener>) getDefaultFuture();
            if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(defaultFuture);
            return defaultFuture;
        }

        try {
            final int dataSize = ((CachedData) value).getData().length;

            if (enableChunking.get()) {
                if (dataSize > chunkSize.get()) {
                    final CachedData[] cd = createChunks(value, key);
                    final int len = cd.length;
                    final OperationFuture<Boolean>[] futures = new OperationFuture[len];
                    for (int i = 0; i < cd.length; i++) {
                        final String prefix = (i < 10) ? "0" : "";
                        futures[i] = evcacheMemcachedClient.set(key + "_" + prefix + i, timeToLive, cd[i], null, null);
                    }
                    // ensure we are deleting the unchunked key if it exists. 
                    // Ignore return value since it may not exist.
                    evcacheMemcachedClient.delete(key);
                    return new EVCacheFutures(futures, key, appName, serverGroup, evcacheLatch);
                } else {
                    // delete all the chunks if they exist as the
                    // data is moving from chunked to unchunked
                    delete(key);
                    return evcacheMemcachedClient.set(key, timeToLive, value, null, evcacheLatch);
                }
            } else if(shouldHashKey()) {
                final String hKey = getHashedKey(key);
                final CachedData cVal = getEVCacheValue(key, value, timeToLive);
                return evcacheMemcachedClient.set(hKey, timeToLive, cVal, null, evcacheLatch);
            } else {
                return evcacheMemcachedClient.set(key, timeToLive, value, null, evcacheLatch);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    protected CachedData getEVCacheValue(String key, CachedData cData, int timeToLive) {
        final EVCacheValue val = new EVCacheValue(key, cData.getData(), cData.getFlags(), timeToLive, System.currentTimeMillis());
        return evcacheValueTranscoder.encode(val);
    }

    protected boolean shouldHashKey() {
        return (!hashKeyByApp.get() && hashKeyByServerGroup.get());
    }
    
    protected String getHashedKey(String key) {
        return KeyHasher.getHashedKey(key, hashingAlgo.get());
    }

    public <T> Future<Boolean> appendOrAdd(String key, CachedData value, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key)) {
            if (log.isInfoEnabled()) log.info("Node : " + node + " is not active. Failing fast and dropping the write event.");
            final ListenableFuture<Boolean, OperationCompletionListener> defaultFuture = (ListenableFuture<Boolean, OperationCompletionListener>) getDefaultFuture();
            if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(defaultFuture);
            return defaultFuture;
        }

        try {
            return evcacheMemcachedClient.asyncAppendOrAdd(key, timeToLive, value, evcacheLatch);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public Future<Boolean> replace(String key, CachedData cd, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        return _replace(key, cd, timeToLive, evcacheLatch);
    }

    @Deprecated
    public <T> Future<Boolean> replace(String key, T value, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        final CachedData cd;
        if (value instanceof CachedData) {
            cd = (CachedData) value;
        } else {
            cd = getTranscoder().encode(value);
        }
        return _replace(key, cd, timeToLive, evcacheLatch);
    }

    private Future<Boolean> _replace(String key, CachedData value, int timeToLive, EVCacheLatch evcacheLatch) throws Exception {
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key)) {
            if (log.isInfoEnabled()) log.info("Node : " + node + " is not active. Failing fast and dropping the replace event.");
            final ListenableFuture<Boolean, OperationCompletionListener> defaultFuture = (ListenableFuture<Boolean, OperationCompletionListener>) getDefaultFuture();
            if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(defaultFuture);
            return defaultFuture;
        }

        try {
            final int dataSize = ((CachedData) value).getData().length;
            if (enableChunking.get() && dataSize > chunkSize.get()) {
                final CachedData[] cd = createChunks(value, key);
                final int len = cd.length;
                final OperationFuture<Boolean>[] futures = new OperationFuture[len];
                for (int i = 0; i < cd.length; i++) {
                    final String prefix = (i < 10) ? "0" : "";
                    futures[i] = evcacheMemcachedClient.replace(key + "_" + prefix + i, timeToLive, cd[i], null, null);
                }
                return new EVCacheFutures(futures, key, appName, serverGroup, evcacheLatch);
            } else if(shouldHashKey()) {
                final String hKey = getHashedKey(key);
                final CachedData cVal = getEVCacheValue(key, value, timeToLive);
                return evcacheMemcachedClient.replace(hKey, timeToLive, cVal, null, evcacheLatch);
            } else {
                return evcacheMemcachedClient.replace(key, timeToLive, value, null, evcacheLatch);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }


    /*
    public boolean appendOrAdd(String key, CachedData value, int timeToLive) throws EVCacheException {
        int i = 0;
        try {
            do {
                final Future<Boolean> future = evcacheMemcachedClient.append(key, value);
                try {
                    if(future.get(operationTimeout.get(), TimeUnit.MILLISECONDS) == Boolean.FALSE) {
                        final Future<Boolean> f = evcacheMemcachedClient.add(key, timeToLive, value);
                        if(f.get(operationTimeout.get(), TimeUnit.MILLISECONDS) == Boolean.TRUE) {
                            return true;
                        }
                    } else {
                        return true;
                    }
                } catch(TimeoutException te) {
                    return false;
                }
            } while(i++ < 2);
        } catch (Exception ex) {
            if (log.isDebugEnabled() ) log.debug("Exception appendOrAdd data for APP " + appName + ", key : " + key, ex);
            return false;
        } 
        return false;
    }
    */

    private Future<Boolean> _add(String key, int exp, CachedData value, EVCacheLatch latch) throws Exception {
        if (enableChunking.get()) throw new EVCacheException("This operation is not supported as chunking is enabled on this EVCacheClient.");
        if (addCounter == null) addCounter = EVCacheMetricsFactory.getCounter(serverGroup.getName() + "-AddCall");

        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key)) return getDefaultFuture();

        addCounter.increment();
        if(shouldHashKey()) {
            final String hKey = getHashedKey(key);
            final CachedData cVal = getEVCacheValue(key, value, exp);
            return evcacheMemcachedClient.add(hKey, exp, cVal, null, latch);
        } else {
            return evcacheMemcachedClient.add(key, exp, value, null, latch);
        }
    }

    @Deprecated
    public <T> Future<Boolean> add(String key, int exp, T value) throws Exception {
        final CachedData cd;
        if (value instanceof CachedData) {
            cd = (CachedData) value;
        } else {
            cd = getTranscoder().encode(value);
        }
        return _add(key, exp, cd, null);
    }

    @Deprecated
    public <T> Future<Boolean> add(String key, int exp, T value, Transcoder<T> tc) throws Exception {
        final CachedData cd;
        if (value instanceof CachedData) {
            cd = (CachedData) value;
        } else {
            if(tc == null) {
                cd = getTranscoder().encode(value);
            } else {
                cd = tc.encode(value);
            }
        }
        return _add(key, exp, cd, null);
    }

    @Deprecated
    public <T> Future<Boolean> add(String key, int exp, T value, final Transcoder<T> tc, EVCacheLatch latch)  throws Exception {
        final CachedData cd;
        if (value instanceof CachedData) {
            cd = (CachedData) value;
        } else {
            if(tc == null) {
                cd = getTranscoder().encode(value);
            } else {
                cd = tc.encode(value);
            }
        }
        return _add(key, exp, cd, latch);
    }

    public Future<Boolean> add(String key, int exp, CachedData value, EVCacheLatch latch)  throws Exception {
        return _add(key, exp, value, latch);
    }

    public <T> Future<Boolean> touch(String key, int timeToLive) throws Exception {
    	return touch(key, timeToLive, null);
    }

    public <T> Future<Boolean> touch(String key, int timeToLive, EVCacheLatch latch) throws Exception {
    	if(ignoreTouch.get()) {
    		final ListenableFuture<Boolean, OperationCompletionListener> sf = new SuccessFuture();
    		if (latch != null && latch instanceof EVCacheLatchImpl && !isInWriteOnly()) ((EVCacheLatchImpl) latch).addFuture(sf);
    		return sf;
    	}
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key)) {
            final ListenableFuture<Boolean, OperationCompletionListener> defaultFuture = (ListenableFuture<Boolean, OperationCompletionListener>) getDefaultFuture();
            if (latch != null && latch instanceof EVCacheLatchImpl && !isInWriteOnly()) ((EVCacheLatchImpl) latch).addFuture(defaultFuture);
            return defaultFuture;
        }

        if (enableChunking.get()) {
            final ChunkDetails<?> cd = getChunkDetails(key);
            if (cd.isChunked()) {
                final List<String> keys = cd.getChunkKeys();
                OperationFuture<Boolean>[] futures = new OperationFuture[keys.size() + 1];
                futures[0] = evcacheMemcachedClient.touch(key + "_00", timeToLive, latch);
                for (int i = 0; i < keys.size(); i++) {
                    final String prefix = (i < 10) ? "0" : "";
                    final String _key = key + "_" + prefix + i;
                    futures[i + 1] = evcacheMemcachedClient.touch(_key, timeToLive, latch);
                }
                return new EVCacheFutures(futures, key, appName, serverGroup, latch);
            } else {
                return evcacheMemcachedClient.touch(key, timeToLive, latch);
            }
        } else if(shouldHashKey()) {
            final String hKey = getHashedKey(key);
            return evcacheMemcachedClient.touch(hKey, timeToLive, latch);
        } else {
            return evcacheMemcachedClient.touch(key, timeToLive, latch);
        }
    }

    public <T> Future<T> asyncGet(String key, Transcoder<T> tc, boolean _throwException, boolean hasZF)
            throws Exception {
        if (enableChunking.get()) throw new EVCacheException(
                "This operation is not supported as chunking is enabled on this EVCacheClient.");
        if (!validateNode(key, _throwException)) return null;
        if (tc == null) tc = (Transcoder<T>) getTranscoder();
        if(shouldHashKey()) {
            final String hKey = getHashedKey(key);
            return evcacheMemcachedClient.asyncGet(hKey, tc, null);
        } else {
            return evcacheMemcachedClient.asyncGet(key, tc, null);
        }
    }

    public Future<Boolean> delete(String key) throws Exception {
        return delete(key, null);
    }

    public Future<Boolean> delete(String key, EVCacheLatch latch) throws Exception {
        final MemcachedNode node = evcacheMemcachedClient.getEVCacheNode(key);
        if (!ensureWriteQueueSize(node, key)) {
            final ListenableFuture<Boolean, OperationCompletionListener> defaultFuture = (ListenableFuture<Boolean, OperationCompletionListener>) getDefaultFuture();
            if (latch != null && latch instanceof EVCacheLatchImpl && !isInWriteOnly()) ((EVCacheLatchImpl) latch).addFuture(defaultFuture);
            return defaultFuture;
        }

        if (enableChunking.get()) {
            final ChunkDetails<?> cd = getChunkDetails(key);
            if (cd == null) {
             // Paranoid delete : cases where get fails and we ensure the first key is deleted just in case
                return evcacheMemcachedClient.delete(key + "_00", latch);
            }
            if (!cd.isChunked()) {
                return evcacheMemcachedClient.delete(key, latch);
            } else {
                final List<String> keys = cd.getChunkKeys();
                OperationFuture<Boolean>[] futures = new OperationFuture[keys.size() + 1];
                futures[0] = evcacheMemcachedClient.delete(key + "_00");
                for (int i = 0; i < keys.size(); i++) {
                    futures[i + 1] = evcacheMemcachedClient.delete(keys.get(i), null);
                }
                return new EVCacheFutures(futures, key, appName, serverGroup, latch);
            }
        } else if(shouldHashKey()) {
            final String hKey = getHashedKey(key);
            return evcacheMemcachedClient.delete(hKey, latch);
        } else {
            return evcacheMemcachedClient.delete(key, latch);
        }
    }

    public boolean removeConnectionObserver() {
        try {
            boolean removed = evcacheMemcachedClient.removeObserver(connectionObserver);
            if (removed) connectionObserver = null;
            return removed;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean shutdown(long timeout, TimeUnit unit) {
        shutdown = true;
        try {
            return evcacheMemcachedClient.shutdown(timeout, unit);
        } catch(Throwable t) {
            log.warn("Exception while shutting down", t);
            return true;
        }
    }

    public EVCacheConnectionObserver getConnectionObserver() {
        return this.connectionObserver;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public String getAppName() {
        return appName;
    }

    public String getZone() {
        return zone;
    }

    public int getId() {
        return id;
    }

    public ServerGroup getServerGroup() {
        return serverGroup;
    }

    public String getServerGroupName() {
        return (serverGroup == null ? "NA" : serverGroup.getName());
    }

    public boolean isShutdown() {
        return this.shutdown;
    }
    
    public boolean isInWriteOnly(){
        return pool.isInWriteOnly(getServerGroup());
    }

    /**
     * Return the keys upto the limit. The key will be cannoicalized key( or hashed Key).<br> 
     * <B> The keys are read into memory so make sure you have enough memory to read the specified number of keys<b>
     * @param limit - The number of keys that need to fetched from each memcached clients.
     * @return - the List of keys. 
     */
    public List<String> getAllKeys(final int limit) {
        final List<String> keyList = new ArrayList<String>(limit);
        byte[] array = new byte[EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".all.keys.reader.buffer.size.bytes", 4*1024*1024).get()];
        final int waitInSec = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".all.keys.reader.wait.duration.sec", 60).get();
        for(InetSocketAddress address : memcachedNodesInZone) {
            //final List<String> keyList = new ArrayList<String>(limit);
            Socket socket = null;
            PrintWriter printWriter = null;
            BufferedInputStream bufferedReader = null;
            try {
                socket = new Socket(address.getHostName(), address.getPort());
                printWriter = new PrintWriter(socket.getOutputStream(), true);
                printWriter.print("lru_crawler metadump all \r\n");
                printWriter.print("quit \r\n");
                printWriter.flush();

                bufferedReader = new BufferedInputStream(socket.getInputStream());
                while(isDataAvailableForRead(bufferedReader, waitInSec, TimeUnit.SECONDS, socket)) {
                    int read = bufferedReader.read(array);
                    if (log.isDebugEnabled()) log.debug("Number of bytes read = " +read);
                    if(read > 0) {
                        StringBuilder b = new StringBuilder();
                        boolean start = true;
                        for (int i = 0; i < read; i++) {
                            if(array[i] == ' ') {
                                start = false;
                                if(b.length() > 4) keyList.add(URLDecoder.decode(b.substring(4)));
                                b = new StringBuilder();
                            }
                            if(start) b.append((char)array[i]);
                            if(array[i] == '\n') {
                                start = true;
                            }
                            if(keyList.size() >= limit) {
                                if (log.isDebugEnabled()) log.debug("Record Limit reached. Will break and return");
                                return keyList;
                            }
                        }
                    } else if (read < 0 ){
                        break;
                    }
                }
            } catch (Exception e) {
                if(socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e1) {
                        log.error("Error closing socket", e1);
                    }
                }
                log.error("Exception", e);
            }
            finally {
                if(bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (IOException e1) {
                        log.error("Error closing bufferedReader", e1);
                    }
                }
                if(printWriter != null) {
                    try {
                        printWriter.close();
                    } catch (Exception e1) {
                        log.error("Error closing socket", e1);
                    }
                }
                if(socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        if (log.isDebugEnabled()) log.debug("Error closing socket", e);
                    }
                }
            }
        }
        return keyList;
    }

    private boolean isDataAvailableForRead(BufferedInputStream bufferedReader, long timeout, TimeUnit unit, Socket socket) throws IOException {
        long expiry = System.currentTimeMillis() + unit.toMillis(timeout);
        int tryCount = 0;
        while(expiry > System.currentTimeMillis()) {
            if(log.isDebugEnabled()) log.debug("For Socket " + socket + " number of bytes available = " + bufferedReader.available() + " and try number is " + tryCount);
            if(bufferedReader.available() > 0) {
                return true;
            }
            if(tryCount++ < 5) {
                try {
                    if(log.isDebugEnabled()) log.debug("Sleep for 100 msec");
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            } else {
                return false;
            }
        }
        return false;
    }    

    public Map<SocketAddress, Map<String, String>> getStats(String cmd) {
        if(config.isRendInstance()) {
            List<InetSocketAddress> udsproxyInetSocketAddress = new ArrayList<InetSocketAddress>(memcachedNodesInZone.size());
            for(InetSocketAddress address : memcachedNodesInZone) {
                udsproxyInetSocketAddress.add(new InetSocketAddress(address.getHostName(), config.getUdsproxyMemcachedPort()));
            }
            
            MemcachedClient mc = null;
            try {
                mc = new MemcachedClient(connectionFactory, udsproxyInetSocketAddress);
                return mc.getStats(cmd);
            } catch(Exception ex) {
                
            } finally {
                if(mc != null) mc.shutdown();
            }
            return Collections.<SocketAddress, Map<String, String>>emptyMap();
        } else {
            return evcacheMemcachedClient.getStats(cmd);
        }
    }

    public Map<SocketAddress, String> getVersions() {
        return evcacheMemcachedClient.getVersions();
    }

    public Future<Boolean> flush() {
        return evcacheMemcachedClient.flush();
    }

    public Transcoder<Object> getTranscoder() {
        return evcacheMemcachedClient.getTranscoder();
    }

    public ConnectionFactory getEVCacheConnectionFactory() {
        return this.connectionFactory;
    }

    public NodeLocator getNodeLocator() {
        return this.evcacheMemcachedClient.getNodeLocator();
    }
    
    static class SuccessFuture implements ListenableFuture<Boolean, OperationCompletionListener> {

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return true;
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return true;
		}

		@Override
		public Boolean get() throws InterruptedException, ExecutionException {
			return Boolean.TRUE;
		}

		@Override
		public Boolean get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			return Boolean.TRUE;
		}

        @Override
        public Future<Boolean> addListener(OperationCompletionListener listener) {
            return this;
        }

        @Override
        public Future<Boolean> removeListener(OperationCompletionListener listener) {
            return this;
        }		
    }

    static class DefaultFuture implements ListenableFuture<Boolean, OperationCompletionListener> {
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Boolean get() throws InterruptedException, ExecutionException {
            return Boolean.FALSE;
        }

        @Override
        public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            return Boolean.FALSE;
        }

        @Override
        public Future<Boolean> addListener(OperationCompletionListener listener) {
            return this;
        }

        @Override
        public Future<Boolean> removeListener(OperationCompletionListener listener) {
            return this;
        }
    }

    private Future<Boolean> getDefaultFuture() {
        final Future<Boolean> defaultFuture = new DefaultFuture();
        return defaultFuture;
    }

    public String toString() {
        return "App : " + appName + "; Zone : " + zone + "; Id : " + id + "; " + serverGroup.toString() + "; Nodes : "
                + memcachedNodesInZone.toString();
    }

    public EVCacheMemcachedClient getEVCacheMemcachedClient() {
        return evcacheMemcachedClient;
    }

    public List<InetSocketAddress> getMemcachedNodesInZone() {
        return memcachedNodesInZone;
    }

    public int getMaxWriteQueueSize() {
        return maxWriteQueueSize;
    }

    public ChainedDynamicProperty.IntProperty getReadTimeout() {
        return readTimeout;
    }

    public ChainedDynamicProperty.IntProperty getBulkReadTimeout() {
        return bulkReadTimeout;
    }

    public DynamicIntProperty getMaxReadQueueSize() {
        return maxReadQueueSize;
    }

    public ChainedDynamicProperty.BooleanProperty getEnableChunking() {
        return enableChunking;
    }

    public ChainedDynamicProperty.IntProperty getChunkSize() {
        return chunkSize;
    }

    public ChunkTranscoder getChunkingTranscoder() {
        return chunkingTranscoder;
    }

    public SerializingTranscoder getDecodingTranscoder() {
        return decodingTranscoder;
    }

    public EVCacheClientPool getPool() {
        return pool;
    }

    public Counter getAddCounter() {
        return addCounter;
    }
    
    public EVCacheServerGroupConfig getEVCacheConfig() {
        return config;
    }

   static class ChunkDetails<T> {

        final List<String> chunkKeys;
        final ChunkInfo chunkInfo;
        final boolean chunked;
        final T data;

        public ChunkDetails(List<String> chunkKeys, ChunkInfo chunkInfo, boolean chunked, T data) {
            super();
            this.chunkKeys = chunkKeys;
            this.chunkInfo = chunkInfo;
            this.chunked = chunked;
            this.data = data;
        }

        public List<String> getChunkKeys() {
            return chunkKeys;
        }

        public ChunkInfo getChunkInfo() {
            return chunkInfo;
        }

        public boolean isChunked() {
            return chunked;
        }

        public T getData() {
            return data;
        }

		@Override
		public String toString() {
			return "ChunkDetails [chunkKeys=" + chunkKeys + ", chunkInfo=" + chunkInfo + ", chunked=" + chunked
					+ ", data=" + data + "]";
		}

    }

    static class ChunkInfo {

        final int chunks;
        final int chunkSize;
        final int lastChunk;
        final int flags;
        final String key;
        final long checksum;

        public ChunkInfo(int chunks, int chunkSize, int lastChunk, int flags, String firstKey, long checksum) {
            super();
            this.chunks = chunks;
            this.chunkSize = chunkSize;
            this.lastChunk = lastChunk;
            this.flags = flags;
            this.key = firstKey;
            this.checksum = checksum;
        }

        public int getChunks() {
            return chunks;
        }

        public int getChunkSize() {
            return chunkSize;
        }

        public int getLastChunk() {
            return lastChunk;
        }

        public int getFlags() {
            return flags;
        }

        public String getKey() {
            return key;
        }

        public long getChecksum() {
            return checksum;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{\"chunks\":\"");
            builder.append(chunks);
            builder.append("\",\"chunkSize\":\"");
            builder.append(chunkSize);
            builder.append("\",\"lastChunk\":\"");
            builder.append(lastChunk);
            builder.append("\",\"flags\":\"");
            builder.append(flags);
            builder.append("\",\"key\":\"");
            builder.append(key);
            builder.append("\",\"checksum\":\"");
            builder.append(checksum);
            builder.append("\"}");
            return builder.toString();
        }
    }

    public int getWriteQueueLength() {
        final Collection<MemcachedNode> allNodes = evcacheMemcachedClient.getNodeLocator().getAll();
        int size = 0;
        for(MemcachedNode node : allNodes) {
            if(node instanceof EVCacheNodeImpl) {
                size += ((EVCacheNodeImpl)node).getWriteQueueSize(); 
            }
        }
        return size;
    }

    public int getReadQueueLength() {
        final Collection<MemcachedNode> allNodes = evcacheMemcachedClient.getNodeLocator().getAll();
        int size = 0;
        for(MemcachedNode node : allNodes) {
            if(node instanceof EVCacheNodeImpl) {
                size += ((EVCacheNodeImpl)node).getReadQueueSize(); 
            }
        }
        return size;
    }

    public TagList getTagList() {
        return tags;
    }

}