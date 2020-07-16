/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.consistency.ConsistencyService;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftPeer;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftPeerSet;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.Message;
import com.alibaba.nacos.naming.misc.NamingProxy;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.ServiceStatusSynchronizer;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.Synchronizer;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.push.PushService;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * Core manager storing all services in Nacos
 *
 * ServiceManager是nacos naming server中service核心管理类
 * 在启动时会执行一次本地信息到其他服务器，while(true)发生改变的service队列内容进行本地更新
 * 同时启动对com.alibaba.nacos.naming.domains.meta.前缀的key的监听；在运行过程中，执行controller接受的相关请求的功能，完成本地状态和远程服务器同步
 *
 * 当Service发生变更(增删改，通过事件驱动来解耦)，触发其onChange或onDelete事件(例如DistroConsistencyServiceImpl初始化时，如果是临时节点会触发)，
 * 初始化时延时执行一次ServiceReporter任务来报告各服务的状态信息，一次UpdatedServiceProcessor任务
 *
 * 根据配置来决定是否开启清除空Service的EmptyServiceAutoClean任务(一个延时60s，周期20s执行)，该任务尽量不要频繁触发，以免由于心跳机制而导致服务缓存信息可能被删除然后再次创建Service实例
 * 通过ServiceManage中大量的周期同步任务，来保证服务集群中对等节点的数据最终一致性
 *
 * @author nkorange
 */
@Component
@DependsOn("nacosApplicationContext")
public class ServiceManager implements RecordListener<Service> {

    /**
     *
     * 命名空间         组@@服务名         服务实例
     * Map<namespace, Map<group::serviceName, Service>>
     */
    private Map<String, Map<String, Service>> serviceMap = new ConcurrentHashMap<>();

    private LinkedBlockingDeque<ServiceKey> toBeUpdatedServicesQueue = new LinkedBlockingDeque<>(1024 * 1024);

    private Synchronizer synchronizer = new ServiceStatusSynchronizer();

    private final Lock lock = new ReentrantLock();

    @Resource(name = "consistencyDelegate")
    private ConsistencyService consistencyService;

    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private DistroMapper distroMapper;

    @Autowired
    private ServerListManager serverListManager;

    @Autowired
    private PushService pushService;

    @Autowired
    private RaftPeerSet raftPeerSet;

    @Value("${nacos.naming.empty-service.auto-clean:false}")
    private boolean emptyServiceAutoClean;

    private int maxFinalizeCount = 3;

    private final Object putServiceLock = new Object();

    @Value("${nacos.naming.empty-service.clean.initial-delay-ms:60000}")
    private int cleanEmptyServiceDelay;

    @Value("${nacos.naming.empty-service.clean.period-time-ms:20000}")
    private int cleanEmptyServicePeriod;

    @PostConstruct
    public void init() {

        // bean初始化之后，执行一次调度，调度由ServiceReporter来执行
        // 方法首先遍历serviceMap(Map<namespace, Map<group::serviceName, Service>>),如果size >0 且 distroMapper.responsible(serviceName)为true继续向下执行
        // 通过getService(namespaceId, serviceName)获取到Service实例service
        // 如果service不为空，通过对该service下所有Instance实例组成的ip.getIp() + ":" + ip.getPort() + "_" + ip.getWeight() + "_"+ ip.isHealthy() + "_" + ip.getClusterName()执行md5来获得该service对应的checksum
        // 再将service的checksum封装为Message实例msg,再将该消息同步到除自已以外的所有服务器
        // 同步的过程是通过async http post请求path /nacos/v1/ns/service/status来执行的，此处不关心执行结果，异步执行
        UtilsAndCommons.SERVICE_SYNCHRONIZATION_EXECUTOR.schedule(new ServiceReporter(), 60000, TimeUnit.MILLISECONDS);
        //启动一个新的 UpdatedServiceProcessor 线程并直接执行while (true)，简单粗暴
        // 该循环会一直尝试从LinkedBlockingDeque<ServiceKey>的toBeUpdatedServicesQueue实例来获取需要被更新的ServiceKey
        // 该ServiceKey是由namespaceId+serviceName+serverIP+checksum组成的，而toBeUpdatedServicesQueue就是由外部consumer、provider、console、openapi对实例进行变更时
        // 被该server收到，并offer进去的。获取到ServiceKey后继续向下执行，由GlobalExecutor启动一个新的ServiceUpdater线程，对本地执行线程状态更新updatedHealthStatus(namespaceId, serviceName, serverIP)
        // 具体执行逻辑是通过 ServiceStatusSynchronizer 实例synchronizer的get方法
        // 通过NamingProxy.reqAPI方法对ServiceKey中包含的server发起http同步get调用
        // 调用的path是/nacos/v1/ns/instance/statuses,并获返回result的数据组装成Message实例msg。将get到的实例msg进行解析和对比更新到service
        // 并准备通过 PushService 的 serviceChanged(service) 来将更新推送到连接到本服务器的client
        // 具体过程是通过调用ApplicationContext实例applicationContext的publishEvent()方法，来将封装成的ServiceChangeEvent实例发送出去
        // 需要指出的是为了降低推送频率这里有一个futureMap.containsKey(UtilsAndCommons.assembleFullServiceName(service.getNamespaceId(), service.getName()))的判断，只有为false时才发送事件
        UtilsAndCommons.SERVICE_UPDATE_EXECUTOR.submit(new UpdatedServiceProcessor());

        if (emptyServiceAutoClean) {

            Loggers.SRV_LOG.info("open empty service auto clean job, initialDelay : {} ms, period : {} ms", cleanEmptyServiceDelay, cleanEmptyServicePeriod);

            // delay 60s, period 20s;

            // This task is not recommended to be performed frequently in order to avoid
            // the possibility that the service cache information may just be deleted
            // and then created due to the heartbeat mechanism

            GlobalExecutor.scheduleServiceAutoClean(new EmptyServiceAutoClean(), cleanEmptyServiceDelay, cleanEmptyServicePeriod);
        }

        try {
            Loggers.SRV_LOG.info("listen for service meta change");
            // 该模块主要是启动对service meta change的监听，该模块由代码consistencyService.listen(KeyBuilder.SERVICE_META_KEY_PREFIX, this);来执行
            consistencyService.listen(KeyBuilder.SERVICE_META_KEY_PREFIX, this);
        } catch (NacosException e) {
            Loggers.SRV_LOG.error("listen for service meta change failed!");
        }
    }

    public Map<String, Service> chooseServiceMap(String namespaceId) {
        return serviceMap.get(namespaceId);
    }

    /**
     * 增加到更新队列
     * @param namespaceId
     * @param serviceName
     * @param serverIP
     * @param checksum
     */
    public void addUpdatedService2Queue(String namespaceId, String serviceName, String serverIP, String checksum) {
        lock.lock();
        try {
            toBeUpdatedServicesQueue.offer(new ServiceKey(namespaceId, serviceName, serverIP, checksum), 5, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            toBeUpdatedServicesQueue.poll();
            toBeUpdatedServicesQueue.add(new ServiceKey(namespaceId, serviceName, serverIP, checksum));
            Loggers.SRV_LOG.error("[DOMAIN-STATUS] Failed to add service to be updatd to queue.", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean interests(String key) {
        return KeyBuilder.matchServiceMetaKey(key) && !KeyBuilder.matchSwitchKey(key);
    }

    @Override
    public boolean matchUnlistenKey(String key) {
        return KeyBuilder.matchServiceMetaKey(key) && !KeyBuilder.matchSwitchKey(key);
    }

    @Override
    public void onChange(String key, Service service) throws Exception {
        try {
            if (service == null) {
                Loggers.SRV_LOG.warn("received empty push from raft, key: {}", key);
                return;
            }

            if (StringUtils.isBlank(service.getNamespaceId())) {
                service.setNamespaceId(Constants.DEFAULT_NAMESPACE_ID);
            }

            Loggers.RAFT.info("[RAFT-NOTIFIER] datum is changed, key: {}, value: {}", key, service);

            Service oldDom = getService(service.getNamespaceId(), service.getName());

            if (oldDom != null) {
                oldDom.update(service);
                // re-listen to handle the situation when the underlying listener is removed:
                consistencyService.listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), true), oldDom);
                consistencyService.listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), false), oldDom);
            } else {
                putServiceAndInit(service);
            }
        } catch (Throwable e) {
            Loggers.SRV_LOG.error("[NACOS-SERVICE] error while processing service update", e);
        }
    }

    @Override
    public void onDelete(String key) throws Exception {
        String namespace = KeyBuilder.getNamespace(key);
        String name = KeyBuilder.getServiceName(key);
        Service service = chooseServiceMap(namespace).get(name);
        Loggers.RAFT.info("[RAFT-NOTIFIER] datum is deleted, key: {}", key);

        if (service != null) {
            service.destroy();
            consistencyService.remove(KeyBuilder.buildInstanceListKey(namespace, name, true));

            consistencyService.remove(KeyBuilder.buildInstanceListKey(namespace, name, false));

            consistencyService.unlisten(KeyBuilder.buildServiceMetaKey(namespace, name), service);
            Loggers.SRV_LOG.info("[DEAD-SERVICE] {}", service.toJSON());
        }

        chooseServiceMap(namespace).remove(name);
    }

    private class UpdatedServiceProcessor implements Runnable {
        //get changed service from other server asynchronously
        @Override
        public void run() {
            ServiceKey serviceKey = null;

            try {
                while (true) {
                    try {
                        // 获取要更新的 serviceKey
                        serviceKey = toBeUpdatedServicesQueue.take();
                    } catch (Exception e) {
                        Loggers.EVT_LOG.error("[UPDATE-DOMAIN] Exception while taking item from LinkedBlockingDeque.");
                    }

                    if (serviceKey == null) {
                        continue;
                    }
                    GlobalExecutor.submitServiceUpdate(new ServiceUpdater(serviceKey));
                }
            } catch (Exception e) {
                Loggers.EVT_LOG.error("[UPDATE-DOMAIN] Exception while update service: {}", serviceKey, e);
            }
        }
    }

    /**
     *
     */
    private class ServiceUpdater implements Runnable {

        String namespaceId;
        String serviceName;
        String serverIP;

        public ServiceUpdater(ServiceKey serviceKey) {
            this.namespaceId = serviceKey.getNamespaceId();
            this.serviceName = serviceKey.getServiceName();
            this.serverIP = serviceKey.getServerIP();
        }

        @Override
        public void run() {
            try {
                updatedHealthStatus(namespaceId, serviceName, serverIP);
            } catch (Exception e) {
                Loggers.SRV_LOG.warn("[DOMAIN-UPDATER] Exception while update service: {} from {}, error: {}",
                    serviceName, serverIP, e);
            }
        }
    }

    public int getPagedClusterState(String namespaceId, int startPage, int pageSize, String keyword, List<RaftPeer> raftPeerList) {

        List<RaftPeer> matchList = new ArrayList<>();
        RaftPeer localRaftPeer = raftPeerSet.local();
        matchList.add(localRaftPeer);
        Set<String> otherServerSet = raftPeerSet.allServersWithoutMySelf();
        if (null != otherServerSet && otherServerSet.size() > 0) {
            for (String server: otherServerSet) {
                String path =  UtilsAndCommons.NACOS_NAMING_OPERATOR_CONTEXT + UtilsAndCommons.NACOS_NAMING_CLUSTER_CONTEXT + "/state";
                Map<String, String> params = Maps.newHashMapWithExpectedSize(2);
                try {
                    String content = NamingProxy.reqCommon(path, params, server, false);
                    if (!StringUtils.EMPTY.equals(content)) {
                        RaftPeer raftPeer = JSONObject.parseObject(content, RaftPeer.class);
                        if (null != raftPeer) {
                            matchList.add(raftPeer);
                        }
                    }
                } catch (Exception e) {
                    Loggers.SRV_LOG.warn("[QUERY-CLUSTER-STATE] Exception while query cluster state from {}, error: {}",
                        server, e);
                }
            }
        }
        List<RaftPeer> tempList = new ArrayList<>();
        if (StringUtils.isNotBlank(keyword)) {
            for (RaftPeer raftPeer : matchList) {
                String ip = raftPeer.ip.split(":")[0];
                if (keyword.equals(ip)) {
                    tempList.add(raftPeer);
                }
            }
            matchList = tempList;
        }

        if (pageSize >= matchList.size()) {
            raftPeerList.addAll(matchList);
            return matchList.size();
        }

        for (int i = 0; i < matchList.size(); i++) {
            if (i < startPage * pageSize) {
                continue;
            }

            raftPeerList.add(matchList.get(i));

            if (raftPeerList.size() >= pageSize) {
                break;
            }
        }

        return matchList.size();
    }

    public RaftPeer getMySelfClusterState() {
        return raftPeerSet.local();
    }

    public void updatedHealthStatus(String namespaceId, String serviceName, String serverIP) {
        Message msg = synchronizer.get(serverIP, UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName));
        JSONObject serviceJson = JSON.parseObject(msg.getData());

        JSONArray ipList = serviceJson.getJSONArray("ips");
        Map<String, String> ipsMap = new HashMap<>(ipList.size());
        for (int i = 0; i < ipList.size(); i++) {

            String ip = ipList.getString(i);
            String[] strings = ip.split("_");
            ipsMap.put(strings[0], strings[1]);
        }

        Service service = getService(namespaceId, serviceName);

        if (service == null) {
            return;
        }

        boolean changed = false;

        List<Instance> instances = service.allIPs();
        for (Instance instance : instances) {

            boolean valid = Boolean.parseBoolean(ipsMap.get(instance.toIPAddr()));
            if (valid != instance.isHealthy()) {
                changed = true;
                instance.setHealthy(valid);
                Loggers.EVT_LOG.info("{} {SYNC} IP-{} : {}:{}@{}",
                    serviceName, (instance.isHealthy() ? "ENABLED" : "DISABLED"),
                    instance.getIp(), instance.getPort(), instance.getClusterName());
            }
        }

        if (changed) {
            pushService.serviceChanged(service);
        }

        StringBuilder stringBuilder = new StringBuilder();
        List<Instance> allIps = service.allIPs();
        for (Instance instance : allIps) {
            stringBuilder.append(instance.toIPAddr()).append("_").append(instance.isHealthy()).append(",");
        }

        if (changed && Loggers.EVT_LOG.isDebugEnabled()) {
            Loggers.EVT_LOG.debug("[HEALTH-STATUS-UPDATED] namespace: {}, service: {}, ips: {}",
                service.getNamespaceId(), service.getName(), stringBuilder.toString());
        }

    }

    public Set<String> getAllServiceNames(String namespaceId) {
        return serviceMap.get(namespaceId).keySet();
    }

    public Map<String, Set<String>> getAllServiceNames() {

        Map<String, Set<String>> namesMap = new HashMap<>(16);
        for (String namespaceId : serviceMap.keySet()) {
            namesMap.put(namespaceId, serviceMap.get(namespaceId).keySet());
        }
        return namesMap;
    }

    public Set<String> getAllNamespaces() {
        return serviceMap.keySet();
    }

    public List<String> getAllServiceNameList(String namespaceId) {
        if (chooseServiceMap(namespaceId) == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(chooseServiceMap(namespaceId).keySet());
    }

    public Map<String, Set<Service>> getResponsibleServices() {
        Map<String, Set<Service>> result = new HashMap<>(16);
        for (String namespaceId : serviceMap.keySet()) {
            result.put(namespaceId, new HashSet<>());
            for (Map.Entry<String, Service> entry : serviceMap.get(namespaceId).entrySet()) {
                Service service = entry.getValue();
                if (distroMapper.responsible(entry.getKey())) {
                    result.get(namespaceId).add(service);
                }
            }
        }
        return result;
    }

    public int getResponsibleServiceCount() {
        int serviceCount = 0;
        for (String namespaceId : serviceMap.keySet()) {
            for (Map.Entry<String, Service> entry : serviceMap.get(namespaceId).entrySet()) {
                if (distroMapper.responsible(entry.getKey())) {
                    serviceCount++;
                }
            }
        }
        return serviceCount;
    }

    public int getResponsibleInstanceCount() {
        Map<String, Set<Service>> responsibleServices = getResponsibleServices();
        int count = 0;
        for (String namespaceId : responsibleServices.keySet()) {
            for (Service service : responsibleServices.get(namespaceId)) {
                count += service.allIPs().size();
            }
        }

        return count;
    }

    public void easyRemoveService(String namespaceId, String serviceName) throws Exception {

        Service service = getService(namespaceId, serviceName);
        if (service == null) {
            throw new IllegalArgumentException("specified service not exist, serviceName : " + serviceName);
        }

        consistencyService.remove(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName));
    }

    public void addOrReplaceService(Service service) throws NacosException {
        consistencyService.put(KeyBuilder.buildServiceMetaKey(service.getNamespaceId(), service.getName()), service);
    }

    public void createEmptyService(String namespaceId, String serviceName, boolean local) throws NacosException {
        createServiceIfAbsent(namespaceId, serviceName, local, null);
    }

    public void createServiceIfAbsent(String namespaceId, String serviceName, boolean local, Cluster cluster) throws NacosException {
        // 从缓存中获取服务实例
        Service service = getService(namespaceId, serviceName);
        if (service == null) {
            // 服务不存在的时候，创建一个
            Loggers.SRV_LOG.info("creating empty service {}:{}", namespaceId, serviceName);
            service = new Service();
            service.setName(serviceName);
            service.setNamespaceId(namespaceId);
            service.setGroupName(NamingUtils.getGroupName(serviceName));
            // now validate the service. if failed, exception will be thrown
            service.setLastModifiedMillis(System.currentTimeMillis());
            service.recalculateChecksum();
            if (cluster != null) {
                cluster.setService(service);
                service.getClusterMap().put(cluster.getName(), cluster);
            }
            //服务验证，服务和集群名验证
            service.validate();
            // 将新建的服务，初始化，并存入缓存
            putServiceAndInit(service);
            // local判断是不是你临时节点，临时节点为true
            if (!local) {
                addOrReplaceService(service);
            }
        }
    }

    /**
     *
     * service-> cluster -> instance
     * Register an instance to a service in AP mode.
     * <p>
     * This method creates service or cluster silently if they don't exist.
     *
     * @param namespaceId id of namespace
     * @param serviceName service name
     * @param instance    instance to register
     * @throws Exception any error occurred in the process
     */
    public void registerInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {

        //判断本地缓存中是否存在该命名空间，如果不存在就创建
        //之后判断该命名空间下是否存在该服务，如果不存在就创建空的服务
        //注意这里并没有更新服务的实例信息
        createEmptyService(namespaceId, serviceName, instance.isEphemeral());

        Service service = getService(namespaceId, serviceName);

        if (service == null) {
            throw new NacosException(NacosException.INVALID_PARAM,
                "service not found, namespace: " + namespaceId + ", service: " + serviceName);
        }
        // 增加服务实例
        addInstance(namespaceId, serviceName, instance.isEphemeral(), instance);
    }

    public void updateInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {

        Service service = getService(namespaceId, serviceName);

        if (service == null) {
            throw new NacosException(NacosException.INVALID_PARAM,
                "service not found, namespace: " + namespaceId + ", service: " + serviceName);
        }

        if (!service.allIPs().contains(instance)) {
            throw new NacosException(NacosException.INVALID_PARAM, "instance not exist: " + instance);
        }

        addInstance(namespaceId, serviceName, instance.isEphemeral(), instance);
    }

    public void addInstance(String namespaceId, String serviceName, boolean ephemeral, Instance... ips) throws NacosException {

        String key = KeyBuilder.buildInstanceListKey(namespaceId, serviceName, ephemeral);
        // namespaceId以及serviceName获取service对象，并将Instance[]数组注册到service中
        Service service = getService(namespaceId, serviceName);
        // 这里再判断下service是否为null比较好，因为可能这个时候也为空，synchronized锁空对象会报异常，空对象没monitor了
        synchronized (service) {
            //获取所有该服务的实例
            List<Instance> instanceList = addIpAddresses(service, ephemeral, ips);

            Instances instances = new Instances();
            instances.setInstanceList(instanceList);

            consistencyService.put(key, instances);
        }
    }

    public void removeInstance(String namespaceId, String serviceName, boolean ephemeral, Instance... ips) throws NacosException {
        Service service = getService(namespaceId, serviceName);

        synchronized (service) {
            removeInstance(namespaceId, serviceName, ephemeral, service, ips);
        }
    }

    public void removeInstance(String namespaceId, String serviceName, boolean ephemeral, Service service, Instance... ips) throws NacosException {

        String key = KeyBuilder.buildInstanceListKey(namespaceId, serviceName, ephemeral);

        List<Instance> instanceList = substractIpAddresses(service, ephemeral, ips);

        Instances instances = new Instances();
        instances.setInstanceList(instanceList);

        consistencyService.put(key, instances);
    }

    public Instance getInstance(String namespaceId, String serviceName, String cluster, String ip, int port) {
        Service service = getService(namespaceId, serviceName);
        if (service == null) {
            return null;
        }

        List<String> clusters = new ArrayList<>();
        clusters.add(cluster);

        List<Instance> ips = service.allIPs(clusters);
        if (ips == null || ips.isEmpty()) {
            return null;
        }

        for (Instance instance : ips) {
            if (instance.getIp().equals(ip) && instance.getPort() == port) {
                return instance;
            }
        }

        return null;
    }

    /**
     * 其实就是更新Instance注册后，需要更新Instance实例的addr信息，然后将更新后的Instance地址列表信息返回，
     * 更新Service下的Instance的列表信息，
     * 也就是刚刚的addInstance(String namespaceId, String serviceName, boolean ephemeral, Instance... ips)函数的后面三行代码
     * 到此，Instance从Nacos Client端注册到Nacos Server的流程就完了
     * @param service
     * @param action
     * @param ephemeral
     * @param ips
     * @return
     * @throws NacosException
     */
    public List<Instance> updateIpAddresses(Service service, String action, boolean ephemeral, Instance... ips) throws NacosException {
        // 获得老的数据实例集合
        Datum datum = consistencyService.get(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), ephemeral));
        //获取集群中所有相关的实例集合，临时的或者是永久的
        List<Instance> currentIPs = service.allIPs(ephemeral);
        //IP端口和实例的映射
        Map<String, Instance> currentInstances = new HashMap<>(currentIPs.size());
        // 当前实例id的集合
        Set<String> currentInstanceIds = Sets.newHashSet();

        for (Instance instance : currentIPs) {
            currentInstances.put(instance.toIPAddr(), instance);
            currentInstanceIds.add(instance.getInstanceId());
        }

        //更新后的老的实例集合
        Map<String, Instance> instanceMap;
        //根据当前服务实例的健康标志和心跳时间，来更新老的实例集合数据
        if (datum != null) {
            instanceMap = setValid(((Instances) datum.value).getInstanceList(), currentInstances);
        } else {
            instanceMap = new HashMap<>(ips.length);
        }

        for (Instance instance : ips) {
            //不存在就创建服务实例集群
            if (!service.getClusterMap().containsKey(instance.getClusterName())) {
                Cluster cluster = new Cluster(instance.getClusterName(), service);
                // 初始化，开启集群心跳检测
                cluster.init();
                // 添加服务实例
                service.getClusterMap().put(instance.getClusterName(), cluster);
                Loggers.SRV_LOG.warn("cluster: {} not found, ip: {}, will create new cluster with default configuration.",
                    instance.getClusterName(), instance.toJSON());
            }
            //删除操作的话就删除老的实例集合的数据
            if (UtilsAndCommons.UPDATE_INSTANCE_ACTION_REMOVE.equals(action)) {
                instanceMap.remove(instance.getDatumKey());
            } else {
                instance.setInstanceId(instance.generateInstanceId(currentInstanceIds));
                instanceMap.put(instance.getDatumKey(), instance);
            }

        }

        if (instanceMap.size() <= 0 && UtilsAndCommons.UPDATE_INSTANCE_ACTION_ADD.equals(action)) {
            throw new IllegalArgumentException("ip list can not be empty, service: " + service.getName() + ", ip list: "
                + JSON.toJSONString(instanceMap.values()));
        }

        return new ArrayList<>(instanceMap.values());
    }

    public List<Instance> substractIpAddresses(Service service, boolean ephemeral, Instance... ips) throws NacosException {
        return updateIpAddresses(service, UtilsAndCommons.UPDATE_INSTANCE_ACTION_REMOVE, ephemeral, ips);
    }

    public List<Instance> addIpAddresses(Service service, boolean ephemeral, Instance... ips) throws NacosException {
        return updateIpAddresses(service, UtilsAndCommons.UPDATE_INSTANCE_ACTION_ADD, ephemeral, ips);
    }

    private Map<String, Instance> setValid(List<Instance> oldInstances, Map<String, Instance> map) {

        Map<String, Instance> instanceMap = new HashMap<>(oldInstances.size());
        for (Instance instance : oldInstances) {
            Instance instance1 = map.get(instance.toIPAddr());
            if (instance1 != null) {
                instance.setHealthy(instance1.isHealthy());
                instance.setLastBeat(instance1.getLastBeat());
            }
            instanceMap.put(instance.getDatumKey(), instance);
        }
        return instanceMap;
    }

    public Service getService(String namespaceId, String serviceName) {
        if (serviceMap.get(namespaceId) == null) {
            return null;
        }
        return chooseServiceMap(namespaceId).get(serviceName);
    }

    public boolean containService(String namespaceId, String serviceName) {
        return getService(namespaceId, serviceName) != null;
    }

    public void putService(Service service) {
        //如果还没有命名空间就增加命名一个空间
        if (!serviceMap.containsKey(service.getNamespaceId())) {
            synchronized (putServiceLock) {
                if (!serviceMap.containsKey(service.getNamespaceId())) {
                    serviceMap.put(service.getNamespaceId(), new ConcurrentHashMap<>(16));
                }
            }
        }
        serviceMap.get(service.getNamespaceId()).put(service.getName(), service);
    }

    private void putServiceAndInit(Service service) throws NacosException {
        // 创建本地服务缓存
        putService(service);
        // 启动一个定时检测的任务
        service.init();
        // 如果是一个临时的Instance，则Nacos Server会为其设置一致性consistencyService的listener
        consistencyService.listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), true), service);
        consistencyService.listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), false), service);
        Loggers.SRV_LOG.info("[NEW-SERVICE] {}", service.toJSON());
    }


    public List<Service> searchServices(String namespaceId, String regex) {
        List<Service> result = new ArrayList<>();
        for (Map.Entry<String, Service> entry : chooseServiceMap(namespaceId).entrySet()) {
            Service service = entry.getValue();
            String key = service.getName() + ":" + ArrayUtils.toString(service.getOwners());
            if (key.matches(regex)) {
                result.add(service);
            }
        }

        return result;
    }

    public int getServiceCount() {
        int serviceCount = 0;
        for (String namespaceId : serviceMap.keySet()) {
            serviceCount += serviceMap.get(namespaceId).size();
        }
        return serviceCount;
    }

    public int getInstanceCount() {
        int total = 0;
        for (String namespaceId : serviceMap.keySet()) {
            for (Service service : serviceMap.get(namespaceId).values()) {
                total += service.allIPs().size();
            }
        }
        return total;
    }

    public Map<String, Service> getServiceMap(String namespaceId) {
        return serviceMap.get(namespaceId);
    }

    public int getPagedService(String namespaceId, int startPage, int pageSize, String param, String containedInstance, List<Service> serviceList, boolean hasIpCount) {

        List<Service> matchList;

        if (chooseServiceMap(namespaceId) == null) {
            return 0;
        }

        if (StringUtils.isNotBlank(param)) {
            StringJoiner regex = new StringJoiner(Constants.SERVICE_INFO_SPLITER);
            for (String s : param.split(Constants.SERVICE_INFO_SPLITER)) {
                regex.add(StringUtils.isBlank(s) ? Constants.ANY_PATTERN : Constants.ANY_PATTERN + s + Constants.ANY_PATTERN);
            }
            matchList = searchServices(namespaceId, regex.toString());
        } else {
            matchList = new ArrayList<>(chooseServiceMap(namespaceId).values());
        }

        if (!CollectionUtils.isEmpty(matchList) && hasIpCount) {
            matchList = matchList.stream().filter(s -> !CollectionUtils.isEmpty(s.allIPs())).collect(Collectors.toList());
        }

        if (StringUtils.isNotBlank(containedInstance)) {

            boolean contained;
            for (int i = 0; i < matchList.size(); i++) {
                Service service = matchList.get(i);
                contained = false;
                List<Instance> instances = service.allIPs();
                for (Instance instance : instances) {
                    if (containedInstance.contains(":")) {
                        if (StringUtils.equals(instance.getIp() + ":" + instance.getPort(), containedInstance)) {
                            contained = true;
                            break;
                        }
                    } else {
                        if (StringUtils.equals(instance.getIp(), containedInstance)) {
                            contained = true;
                            break;
                        }
                    }
                }
                if (!contained) {
                    matchList.remove(i);
                    i--;
                }
            }
        }

        if (pageSize >= matchList.size()) {
            serviceList.addAll(matchList);
            return matchList.size();
        }

        for (int i = 0; i < matchList.size(); i++) {
            if (i < startPage * pageSize) {
                continue;
            }

            serviceList.add(matchList.get(i));

            if (serviceList.size() >= pageSize) {
                break;
            }
        }

        return matchList.size();
    }

    public static class ServiceChecksum {

        public String namespaceId;
        public Map<String, String> serviceName2Checksum = new HashMap<String, String>();

        public ServiceChecksum() {
            this.namespaceId = Constants.DEFAULT_NAMESPACE_ID;
        }

        public ServiceChecksum(String namespaceId) {
            this.namespaceId = namespaceId;
        }

        public void addItem(String serviceName, String checksum) {
            if (StringUtils.isEmpty(serviceName) || StringUtils.isEmpty(checksum)) {
                Loggers.SRV_LOG.warn("[DOMAIN-CHECKSUM] serviceName or checksum is empty,serviceName: {}, checksum: {}",
                    serviceName, checksum);
                return;
            }
            serviceName2Checksum.put(serviceName, checksum);
        }
    }


    private class EmptyServiceAutoClean implements Runnable {

        @Override
        public void run() {

            // Parallel flow opening threshold

            int parallelSize = 100;

            serviceMap.forEach((namespace, stringServiceMap) -> {
                Stream<Map.Entry<String, Service>> stream = null;
                if (stringServiceMap.size() > parallelSize) {
                    stream = stringServiceMap.entrySet().parallelStream();
                } else {
                    stream = stringServiceMap.entrySet().stream();
                }
                stream
                        .filter(entry -> {
                            final String serviceName = entry.getKey();
                            return distroMapper.responsible(serviceName);
                        })
                        .forEach(entry -> stringServiceMap.computeIfPresent(entry.getKey(), (serviceName, service) -> {
                    if (service.isEmpty()) {

                        // To avoid violent Service removal, the number of times the Service
                        // experiences Empty is determined by finalizeCnt, and if the specified
                        // value is reached, it is removed

                        if (service.getFinalizeCount() > maxFinalizeCount) {
                            Loggers.SRV_LOG.warn("namespace : {}, [{}] services are automatically cleaned",
                                    namespace, serviceName);
                            try {
                                easyRemoveService(namespace, serviceName);
                            } catch (Exception e) {
                                Loggers.SRV_LOG.error("namespace : {}, [{}] services are automatically clean has " +
                                        "error : {}", namespace, serviceName, e);
                            }
                        }

                        service.setFinalizeCount(service.getFinalizeCount() + 1);

                        Loggers.SRV_LOG.debug("namespace : {}, [{}] The number of times the current service experiences " +
                                "an empty instance is : {}", namespace, serviceName, service.getFinalizeCount());
                    } else {
                        service.setFinalizeCount(0);
                    }
                    return service;
                }));
            });
        }
    }

    /**
     * 遍历allServiceNames，取出distroMapper.responsible的serviceName，
     * 重新计算recalculateChecksum，然后添加到ServiceChecksum中，构造Message，
     * 遍历sameSiteServers使用 ServiceStatusSynchronizer.send发送该消息；
     * 最后往 UtilsAndCommons.SERVICE_SYNCHRONIZATION_EXECUTOR 重新注册ServiceReporter
     */
    private class ServiceReporter implements Runnable {

        @Override
        public void run() {
            try {
                // 获得所有的服务
                Map<String, Set<String>> allServiceNames = getAllServiceNames();
                // 如果没有服务，忽略
                if (allServiceNames.size() <= 0) {
                    //ignore
                    return;
                }
                // 遍历服务
                for (String namespaceId : allServiceNames.keySet()) {

                    ServiceChecksum checksum = new ServiceChecksum(namespaceId);

                    for (String serviceName : allServiceNames.get(namespaceId)) {
                        //
                        if (!distroMapper.responsible(serviceName)) {
                            continue;
                        }

                        Service service = getService(namespaceId, serviceName);

                        if (service == null || service.isEmpty()) {
                            continue;
                        }

                        service.recalculateChecksum();

                        checksum.addItem(serviceName, service.getChecksum());
                    }
                    // 构建发送的消息
                    Message msg = new Message();

                    msg.setData(JSON.toJSONString(checksum));

                    List<Server> sameSiteServers = serverListManager.getServers();

                    if (sameSiteServers == null || sameSiteServers.size() <= 0) {
                        return;
                    }

                    for (Server server : sameSiteServers) {
                        // 如果是自己，直接跳过
                        if (server.getKey().equals(NetUtils.localServer())) {
                            continue;
                        }
                        synchronizer.send(server.getKey(), msg);
                    }
                }
            } catch (Exception e) {
                Loggers.SRV_LOG.error("[DOMAIN-STATUS] Exception while sending service status", e);
            } finally {
                UtilsAndCommons.SERVICE_SYNCHRONIZATION_EXECUTOR.schedule(this, switchDomain.getServiceStatusSynchronizationPeriodMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private static class ServiceKey {
        private String namespaceId;
        private String serviceName;
        private String serverIP;
        private String checksum;

        public String getChecksum() {
            return checksum;
        }

        public String getServerIP() {
            return serverIP;
        }

        public String getServiceName() {
            return serviceName;
        }

        public String getNamespaceId() {
            return namespaceId;
        }

        public ServiceKey(String namespaceId, String serviceName, String serverIP, String checksum) {
            this.namespaceId = namespaceId;
            this.serviceName = serviceName;
            this.serverIP = serverIP;
            this.checksum = checksum;
        }

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }
}
