package com.netflix.evcache.pool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.InetAddresses;
import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.config.ChainedDynamicProperty;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicStringSetProperty;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.shared.Application;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.servo.tag.BasicTagList;

public class DiscoveryNodeListProvider implements EVCacheNodeList {
    public static final String DEFAULT_PORT = "11211";
    public static final String DEFAULT_SECURE_PORT = "11443";

    private static Logger log = LoggerFactory.getLogger(DiscoveryNodeListProvider.class);
    private final DiscoveryClient _discoveryClient;
    private final String _appName;
    private final ApplicationInfoManager applicationInfoManager;
    private final Map<String, ChainedDynamicProperty.BooleanProperty> useRendBatchPortMap = new HashMap<String, ChainedDynamicProperty.BooleanProperty>();
    private final DynamicStringSetProperty ignoreHosts;

    public DiscoveryNodeListProvider(ApplicationInfoManager applicationInfoManager, DiscoveryClient discoveryClient, String appName) {
        this.applicationInfoManager = applicationInfoManager;
        this._discoveryClient = discoveryClient;
        this._appName = appName;
        ignoreHosts = new DynamicStringSetProperty(appName + ".ignore.hosts", "");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.evcache.pool.EVCacheNodeList#discoverInstances()
     */
    @Override
    public Map<ServerGroup, EVCacheServerGroupConfig> discoverInstances(String appName) throws IOException {

        if ((applicationInfoManager.getInfo().getStatus() == InstanceStatus.DOWN)) {
            return Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap();
        }

        /* Get a list of EVCACHE instances from the DiscoveryManager */
        final Application app = _discoveryClient.getApplication(_appName);
        if (app == null) return Collections.<ServerGroup, EVCacheServerGroupConfig> emptyMap();

        final List<InstanceInfo> appInstances = app.getInstances();
        final Map<ServerGroup, EVCacheServerGroupConfig> instancesSpecific = new HashMap<ServerGroup, EVCacheServerGroupConfig>();

        /* Iterate all the discovered instances to find usable ones */
        for (InstanceInfo iInfo : appInstances) {
            final DataCenterInfo dcInfo = iInfo.getDataCenterInfo();
            if (dcInfo == null) {
                if (log.isErrorEnabled()) log.error("Data Center Info is null for appName - " + _appName);
                continue;
            }

            /* Only AWS instances are usable; bypass all others */
            if (DataCenterInfo.Name.Amazon != dcInfo.getName() || !(dcInfo instanceof AmazonInfo)) {
                log.error("This is not an AWSDataCenter. You will not be able to use Discovery Nodelist Provider. Cannot proceed. " +
                          "DataCenterInfo : {}; appName - {}. Please use SimpleNodeList provider and specify the server groups manually.",
                          dcInfo, _appName);
                continue;
            }

            final AmazonInfo amznInfo = (AmazonInfo) dcInfo; 
            // We checked above if this instance is Amazon so no need to do a instanceof check
            final String zone = amznInfo.get(AmazonInfo.MetaDataKey.availabilityZone);
            if(zone == null) {
                EVCacheMetricsFactory.increment(_appName, null, "EVCacheClient-DiscoveryNodeListProvider-NULL_ZONE");
                continue;
            }
            final String asgName = iInfo.getASGName();
            if(asgName == null) {
                EVCacheMetricsFactory.increment(_appName, null, "EVCacheClient-DiscoveryNodeListProvider-NULL_SERVER_GROUP");
                continue;
            }

            final DynamicBooleanProperty asgEnabled = EVCacheConfig.getInstance().getDynamicBooleanProperty(asgName + ".enabled", true);
            if (!asgEnabled.get()) {
                if(log.isDebugEnabled()) log.debug("ASG " + asgName + " is disabled so ignoring it");
                continue;
            }

            final Map<String, String> metaInfo = iInfo.getMetadata();
            final int evcachePort = Integer.parseInt((metaInfo != null && metaInfo.containsKey("evcache.port")) ? metaInfo.get("evcache.port") : DEFAULT_PORT);
            final int rendPort = (metaInfo != null && metaInfo.containsKey("rend.port")) ? Integer.parseInt(metaInfo.get("rend.port")) : 0;
            final int rendBatchPort = (metaInfo != null && metaInfo.containsKey("rend.batch.port")) ? Integer.parseInt(metaInfo.get("rend.batch.port")) : 0;
            final int udsproxyMemcachedPort = (metaInfo != null && metaInfo.containsKey("udsproxy.memcached.port")) ? Integer.parseInt(metaInfo.get("udsproxy.memcached.port")) : 0;
            final int udsproxyMementoPort = (metaInfo != null && metaInfo.containsKey("udsproxy.memento.port")) ? Integer.parseInt(metaInfo.get("udsproxy.memento.port")) : 0;

            ChainedDynamicProperty.BooleanProperty useBatchPort = useRendBatchPortMap.get(asgName);
            if (useBatchPort == null) {
                useBatchPort = EVCacheConfig.getInstance().getChainedBooleanProperty(_appName + ".use.batch.port", "evcache.use.batch.port", Boolean.FALSE, null);
                useRendBatchPortMap.put(asgName, useBatchPort);
            }
            int port = rendPort == 0 ? evcachePort : ((useBatchPort.get().booleanValue()) ? rendBatchPort : rendPort);
            final ChainedDynamicProperty.BooleanProperty isSecure = EVCacheConfig.getInstance().getChainedBooleanProperty(asgName + ".use.secure", _appName + ".use.secure", false, null);
            if(isSecure.get()) {
                port = Integer.parseInt((metaInfo != null && metaInfo.containsKey("evcache.secure.port")) ? metaInfo.get("evcache.secure.port") : DEFAULT_SECURE_PORT);
            }

            final ServerGroup serverGroup = new ServerGroup(zone, asgName);
            final Set<InetSocketAddress> instances;
            final EVCacheServerGroupConfig config;
            if (instancesSpecific.containsKey(serverGroup)) {
                config = instancesSpecific.get(serverGroup);
                instances = config.getInetSocketAddress();
            } else {
                instances = new HashSet<InetSocketAddress>();
                config = new EVCacheServerGroupConfig(serverGroup, instances, rendPort, udsproxyMemcachedPort, udsproxyMementoPort);
                instancesSpecific.put(serverGroup, config);
                EVCacheMetricsFactory.getLongGauge(_appName + "-port", BasicTagList.of("ServerGroup", asgName, "APP", _appName)).set(Long.valueOf(port));
            }

            /* Don't try to use downed instances */
            final InstanceStatus status = iInfo.getStatus();
            if (status == null || InstanceStatus.OUT_OF_SERVICE == status || InstanceStatus.DOWN == status) {
                if (log.isDebugEnabled()) log.debug("The Status of the instance in Discovery is " + status + ". App Name : " + _appName + "; Zone : " + zone
                        + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
                continue;
            }

            final InstanceInfo myInfo = applicationInfoManager.getInfo();
            final DataCenterInfo myDC = myInfo.getDataCenterInfo();
            final AmazonInfo myAmznDC = (myDC instanceof AmazonInfo) ? (AmazonInfo) myDC : null;   
            final String myInstanceId = myInfo.getInstanceId();
            final String myIp = myInfo.getIPAddr();
            final String myPublicHostName = (myAmznDC != null) ? myAmznDC.get(AmazonInfo.MetaDataKey.publicHostname) : null;
            boolean isInCloud = false;
            if (myPublicHostName != null) {
                isInCloud = myPublicHostName.startsWith("ec2");
            }

            if (!isInCloud) {
                if (myAmznDC != null && myAmznDC.get(AmazonInfo.MetaDataKey.vpcId) != null) {
                    isInCloud = true;
                } else {
                    if (myIp.equals(myInstanceId)) {
                        isInCloud = false;
                    }
                }
            }
            final String myZone = (myAmznDC != null) ? myAmznDC.get(AmazonInfo.MetaDataKey.availabilityZone) : null;
            final String myRegion = (myZone != null) ? myZone.substring(0, myZone.length() - 1) : null;
            final String region = (zone != null) ? zone.substring(0, zone.length() - 1) : null;
            final String host = amznInfo.get(AmazonInfo.MetaDataKey.publicHostname);
            InetSocketAddress address = null;
            final String vpcId = amznInfo.get(AmazonInfo.MetaDataKey.vpcId);
            final String localIp = amznInfo.get(AmazonInfo.MetaDataKey.localIpv4);
            if (log.isDebugEnabled()) log.debug("myZone - " + myZone + "; zone : " + zone + "; myRegion : " + myRegion + "; region : " + region + "; host : " + host + "; vpcId : " + vpcId);
            if(localIp != null && ignoreHosts.get().contains(localIp)) continue;
            if(host != null && ignoreHosts.get().contains(host)) continue;

            if (vpcId != null) {
                final InetAddress add = InetAddresses.forString(localIp);
                final InetAddress inetAddress = InetAddress.getByAddress(localIp, add.getAddress());
                address = new InetSocketAddress(inetAddress, port);

                if (log.isDebugEnabled()) log.debug("VPC : localIp - " + localIp + " ; add : " + add + "; inetAddress : " + inetAddress + "; address - " + address  
                        + "; App Name : " + _appName + "; Zone : " + zone + "; myZone - " + myZone + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
            } else {
                if(host != null && host.startsWith("ec2")) {

                    final InetAddress inetAddress = (localIp != null) ? InetAddress.getByAddress(host, InetAddresses.forString(localIp).getAddress()) : InetAddress.getByName(host);
                    address = new InetSocketAddress(inetAddress, port);
                    if (log.isDebugEnabled()) log.debug("myZone - " + myZone + ". host : " + host
                            + "; inetAddress : " + inetAddress + "; address - " + address + "; App Name : " + _appName
                            + "; Zone : " + zone + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
                } else {
                    final String ipToUse = (isInCloud) ? localIp : amznInfo.get(AmazonInfo.MetaDataKey.publicIpv4);
                    final InetAddress add = InetAddresses.forString(ipToUse);
                    final InetAddress inetAddress = InetAddress.getByAddress(ipToUse, add.getAddress());
                    address = new InetSocketAddress(inetAddress, port);
                    if (log.isDebugEnabled()) log.debug("CLASSIC : IPToUse - " + ipToUse + " ; add : " + add + "; inetAddress : " + inetAddress + "; address - " + address 
                            + "; App Name : " + _appName + "; Zone : " + zone + "; myZone - " + myZone + "; Host : " + iInfo.getHostName() + "; Instance Id - " + iInfo.getId());
                }
            }

            instances.add(address);
        }
        return instancesSpecific;
    }
}
