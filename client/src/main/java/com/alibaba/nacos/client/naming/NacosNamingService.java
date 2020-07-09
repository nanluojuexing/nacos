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
package com.alibaba.nacos.client.naming;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.selector.AbstractSelector;
import com.alibaba.nacos.client.naming.beat.BeatInfo;
import com.alibaba.nacos.client.naming.beat.BeatReactor;
import com.alibaba.nacos.client.naming.core.Balancer;
import com.alibaba.nacos.client.naming.core.EventDispatcher;
import com.alibaba.nacos.client.naming.core.HostReactor;
import com.alibaba.nacos.client.naming.net.NamingProxy;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.InitUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.client.security.SecurityProxy;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Nacos Naming Service
 *
 * minging 服务
 *
 * @author nkorange
 */
@SuppressWarnings("PMD.ServiceOrDaoClassShouldEndWithImplRule")
public class NacosNamingService implements NamingService {

    /**
     * Each Naming service should have different namespace.
     */
    private String namespace;

    private String endpoint;

    private String serverList;

    private String cacheDir;

    private String logName;

    private HostReactor hostReactor;

    private BeatReactor beatReactor;

    private EventDispatcher eventDispatcher;

    private NamingProxy serverProxy;

    /**
     * 初始化
     * @param serverList
     */
    public NacosNamingService(String serverList) {
        Properties properties = new Properties();
        // 初始化nacos 服务地址
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverList);

        init(properties);
    }

    public NacosNamingService(Properties properties) {
        init(properties);
    }

    /**
     * 初始化数据
     * @param properties
     */
    private void init(Properties properties) {
        // 获取服务
        namespace = InitUtils.initNamespaceForNaming(properties);
        // 初始化服务地址，没有配置 endpoint，使用传入的 ip和端口；
        // 如果有配置 endpoint，则将 serverList 置为空字符串
        // 域名解析规则：在endpoint端配置网段和环境的映射关系，
        // endpoint在接收到客户端的请求之后，根据客户端的来源IP所属网段，计算出该客户端所属环境，
        // 然后找到对应环境的IP列表返回给客户端，如上图二
        // 可以使用 nginx 的geo 模块实现简单的地址服务器
        initServerAddr(properties);
        InitUtils.initWebRootContext();
        // 初始化缓存文件夹 默认为 {user.home} + "/nacos/naming/" + namespace;
        initCacheDir();
        // 初始化log文件
        initLogName(properties);
        // 通过 Notifier 事件监听 监听服务变更
        eventDispatcher = new EventDispatcher();
        //  初始化服务代理 主要做服务地址刷新，和安全校验
        serverProxy = new NamingProxy(namespace, endpoint, serverList, properties);
        // 设置心跳任务线程
        // initClientBeatThreadCount(properties) 初始化客户端心跳检测线程数量，默认值为
        // Runtime.getRuntime().availableProcessors() > 1 ? Runtime.getRuntime().availableProcessors() / 2 : 1;
        // Runtime.getRuntime().availableProcessors() 返回的处理器数量不一定准确
        // 参考： https://blog.csdn.net/zhanghongzheng3213/article/details/83376571
        // 新建 BeatReactor 对象，使用 ScheduledThreadPoolExecutor 创建守护线程，定时运行 BeatProcessor
        beatReactor = new BeatReactor(serverProxy, initClientBeatThreadCount(properties));
        //这里包含3部分 这里 initPollingThreadCount 线程去当前系统可用线程数
        // 1 UpdateTask 定时更新已存在的服务
        // 2 FailoverReactor  容灾备份
        // 3 PushReceiver 接受服务器的udp保文，作出响应

        // initPollingThreadCount(properties) 和上面初始化客户端心跳检测线程数量一样
        // isLoadCacheAtStart(properties) 启动时是否加载缓存的服务信息，默认值 false
        // 新建 HostReactor 对象，HostReactor 创建 ScheduledThreadPoolExecutor 线程池、FailoverReactor 对象和 PushReceiver 对象
        // 其中 FailoverReactor 负责故障转移功能， FailoverReactor init() 代码
        // PushReceiver 使用 DatagramSocket(UDP)，接收消息，如果是 dom 类型，处理接收的数据，
        // 如果服务有变动，添加 serviceInfo 到 EventDispatcher 的 changedServices 里面
        // 更新本地缓存文件，更新 MetricsMonitor 中服务数量，返回 ack 信息
        hostReactor = new HostReactor(eventDispatcher, serverProxy, cacheDir, isLoadCacheAtStart(properties),
            initPollingThreadCount(properties));
    }

    private int initClientBeatThreadCount(Properties properties) {
        if (properties == null) {
            return UtilAndComs.DEFAULT_CLIENT_BEAT_THREAD_COUNT;
        }

        return NumberUtils.toInt(properties.getProperty(PropertyKeyConst.NAMING_CLIENT_BEAT_THREAD_COUNT),
            UtilAndComs.DEFAULT_CLIENT_BEAT_THREAD_COUNT);
    }

    private int initPollingThreadCount(Properties properties) {
        if (properties == null) {

            return UtilAndComs.DEFAULT_POLLING_THREAD_COUNT;
        }

        return NumberUtils.toInt(properties.getProperty(PropertyKeyConst.NAMING_POLLING_THREAD_COUNT),
            UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }

    private boolean isLoadCacheAtStart(Properties properties) {
        boolean loadCacheAtStart = false;
        if (properties != null && StringUtils.isNotEmpty(properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START))) {
            loadCacheAtStart = BooleanUtils.toBoolean(
                properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START));
        }

        return loadCacheAtStart;
    }

    private void initServerAddr(Properties properties) {
        serverList = properties.getProperty(PropertyKeyConst.SERVER_ADDR);
        endpoint = InitUtils.initEndpoint(properties);
        if (StringUtils.isNotEmpty(endpoint)) {
            serverList = "";
        }
    }

    private void initLogName(Properties properties) {
        logName = System.getProperty(UtilAndComs.NACOS_NAMING_LOG_NAME);
        if (StringUtils.isEmpty(logName)) {

            if (properties != null && StringUtils.isNotEmpty(properties.getProperty(UtilAndComs.NACOS_NAMING_LOG_NAME))) {
                logName = properties.getProperty(UtilAndComs.NACOS_NAMING_LOG_NAME);
            } else {
                logName = "naming.log";
            }
        }
    }

    private void initCacheDir() {
        cacheDir = System.getProperty("com.alibaba.nacos.naming.cache.dir");
        if (StringUtils.isEmpty(cacheDir)) {
            cacheDir = System.getProperty("user.home") + "/nacos/naming/" + namespace;
        }
    }
    /***
     * 注册流程
     * NacosServiceRegistry.register()–>namingService.registerInstance()
     * –>serverProxy(NamingProxy).registerService()–>NamingProxy.reqAPI()
     * –>NamingProxy.callServer()–>HttpClient.request();
     */

    /**
     * 注册发布服务   cluster默认
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @throws NacosException
     */
    @Override
    public void registerInstance(String serviceName, String ip, int port) throws NacosException {
        registerInstance(serviceName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }

    @Override
    public void registerInstance(String serviceName, String groupName, String ip, int port) throws NacosException {
        registerInstance(serviceName, groupName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }

    /**
     * 组默认
     * @param serviceName name of service
     * @param ip          instance ip
     * @param port        instance port
     * @param clusterName instance cluster name
     * @throws NacosException
     */
    @Override
    public void registerInstance(String serviceName, String ip, int port, String clusterName) throws NacosException {
        registerInstance(serviceName, Constants.DEFAULT_GROUP, ip, port, clusterName);
    }

    /**
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param ip          instance ip
     * @param port        instance port
     * @param clusterName instance cluster name
     * @throws NacosException
     */
    @Override
    public void registerInstance(String serviceName, String groupName, String ip, int port, String clusterName) throws NacosException {
        // 构建实例信息
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        // 设置权重
        instance.setWeight(1.0);
        instance.setClusterName(clusterName);

        registerInstance(serviceName, groupName, instance);
    }

    @Override
    public void registerInstance(String serviceName, Instance instance) throws NacosException {
        registerInstance(serviceName, Constants.DEFAULT_GROUP, instance);
    }

    /**
     * 这里构建心跳，同步通过代理发布服务
     * @param serviceName name of service
     * @param groupName   group of service
     * @param instance    instance to register
     * @throws NacosException
     */
    @Override
    public void registerInstance(String serviceName, String groupName, Instance instance) throws NacosException {
        // 增加心跳，对于临时实例，默认就是临时实例
        if (instance.isEphemeral()) {
            BeatInfo beatInfo = new BeatInfo();
            beatInfo.setServiceName(NamingUtils.getGroupedName(serviceName, groupName));
            beatInfo.setIp(instance.getIp());
            beatInfo.setPort(instance.getPort());
            beatInfo.setCluster(instance.getClusterName());
            beatInfo.setWeight(instance.getWeight());
            beatInfo.setMetadata(instance.getMetadata());
            beatInfo.setScheduled(false);
            // 设置心跳周期，默认为5秒
            beatInfo.setPeriod(instance.getInstanceHeartBeatInterval());

            beatReactor.addBeatInfo(NamingUtils.getGroupedName(serviceName, groupName), beatInfo);
        }
        // 服务注册
        serverProxy.registerService(NamingUtils.getGroupedName(serviceName, groupName), groupName, instance);
    }


    @Override
    public void deregisterInstance(String serviceName, String ip, int port) throws NacosException {
        deregisterInstance(serviceName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }

    @Override
    public void deregisterInstance(String serviceName, String groupName, String ip, int port) throws NacosException {
        deregisterInstance(serviceName, groupName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }

    @Override
    public void deregisterInstance(String serviceName, String ip, int port, String clusterName) throws NacosException {
        deregisterInstance(serviceName, Constants.DEFAULT_GROUP, ip, port, clusterName);
    }

    @Override
    public void deregisterInstance(String serviceName, String groupName, String ip, int port, String clusterName) throws NacosException {
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setClusterName(clusterName);

        deregisterInstance(serviceName, groupName, instance);
    }

    @Override
    public void deregisterInstance(String serviceName, Instance instance) throws NacosException {
        deregisterInstance(serviceName, Constants.DEFAULT_GROUP, instance);
    }

    @Override
    public void deregisterInstance(String serviceName, String groupName, Instance instance) throws NacosException {
        if (instance.isEphemeral()) {
            beatReactor.removeBeatInfo(NamingUtils.getGroupedName(serviceName, groupName), instance.getIp(), instance.getPort());
        }
        serverProxy.deregisterService(NamingUtils.getGroupedName(serviceName, groupName), instance);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName) throws NacosException {
        return getAllInstances(serviceName, new ArrayList<String>());
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName) throws NacosException {
        return getAllInstances(serviceName, groupName, new ArrayList<String>());
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, boolean subscribe) throws NacosException {
        return getAllInstances(serviceName, new ArrayList<String>(), subscribe);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, boolean subscribe) throws NacosException {
        return getAllInstances(serviceName, groupName, new ArrayList<String>(), subscribe);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, List<String> clusters) throws NacosException {
        return getAllInstances(serviceName, clusters, true);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, List<String> clusters) throws NacosException {
        return getAllInstances(serviceName, groupName, clusters, true);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, List<String> clusters, boolean subscribe)
        throws NacosException {
        return getAllInstances(serviceName, Constants.DEFAULT_GROUP, clusters, subscribe);
    }

    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, List<String> clusters, boolean subscribe) throws NacosException {

        ServiceInfo serviceInfo;
        if (subscribe) {
            serviceInfo = hostReactor.getServiceInfo(NamingUtils.getGroupedName(serviceName, groupName), StringUtils.join(clusters, ","));
        } else {
            serviceInfo = hostReactor.getServiceInfoDirectlyFromServer(NamingUtils.getGroupedName(serviceName, groupName), StringUtils.join(clusters, ","));
        }
        List<Instance> list;
        if (serviceInfo == null || CollectionUtils.isEmpty(list = serviceInfo.getHosts())) {
            return new ArrayList<Instance>();
        }
        return list;
    }

    @Override
    public List<Instance> selectInstances(String serviceName, boolean healthy) throws NacosException {
        return selectInstances(serviceName, new ArrayList<String>(), healthy);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, boolean healthy) throws NacosException {
        return selectInstances(serviceName, groupName, healthy, true);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, boolean healthy, boolean subscribe)
        throws NacosException {
        return selectInstances(serviceName, new ArrayList<String>(), healthy, subscribe);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, boolean healthy, boolean subscribe) throws NacosException {
        return selectInstances(serviceName, groupName, new ArrayList<String>(), healthy, subscribe);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, List<String> clusters, boolean healthy)
        throws NacosException {
        return selectInstances(serviceName, clusters, healthy, true);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, List<String> clusters, boolean healthy) throws NacosException {
        return selectInstances(serviceName, groupName, clusters, healthy, true);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, List<String> clusters, boolean healthy,
                                          boolean subscribe) throws NacosException {
        return selectInstances(serviceName, Constants.DEFAULT_GROUP, clusters, healthy, subscribe);
    }

    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, List<String> clusters, boolean healthy, boolean subscribe) throws NacosException {

        ServiceInfo serviceInfo;
        // 标记是订阅的
        if (subscribe) {
            serviceInfo = hostReactor.getServiceInfo(NamingUtils.getGroupedName(serviceName, groupName), StringUtils.join(clusters, ","));
        } else {
            serviceInfo = hostReactor.getServiceInfoDirectlyFromServer(NamingUtils.getGroupedName(serviceName, groupName), StringUtils.join(clusters, ","));
        }
        return selectInstances(serviceInfo, healthy);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName) throws NacosException {
        return selectOneHealthyInstance(serviceName, new ArrayList<String>());
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName) throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, true);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, boolean subscribe) throws NacosException {
        return selectOneHealthyInstance(serviceName, new ArrayList<String>(), subscribe);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, boolean subscribe) throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, new ArrayList<String>(), subscribe);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, List<String> clusters) throws NacosException {
        return selectOneHealthyInstance(serviceName, clusters, true);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, List<String> clusters) throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, clusters, true);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, List<String> clusters, boolean subscribe)
        throws NacosException {
        return selectOneHealthyInstance(serviceName, Constants.DEFAULT_GROUP, clusters, subscribe);
    }

    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, List<String> clusters, boolean subscribe) throws NacosException {

        if (subscribe) {
            return Balancer.RandomByWeight.selectHost(
                hostReactor.getServiceInfo(NamingUtils.getGroupedName(serviceName, groupName), StringUtils.join(clusters, ",")));
        } else {
            return Balancer.RandomByWeight.selectHost(
                hostReactor.getServiceInfoDirectlyFromServer(NamingUtils.getGroupedName(serviceName, groupName), StringUtils.join(clusters, ",")));
        }
    }

    @Override
    public void subscribe(String serviceName, EventListener listener) throws NacosException {
        subscribe(serviceName, new ArrayList<String>(), listener);
    }

    @Override
    public void subscribe(String serviceName, String groupName, EventListener listener) throws NacosException {
        subscribe(serviceName, groupName, new ArrayList<String>(), listener);
    }

    @Override
    public void subscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException {
        subscribe(serviceName, Constants.DEFAULT_GROUP, clusters, listener);
    }

    @Override
    public void subscribe(String serviceName, String groupName, List<String> clusters, EventListener listener) throws NacosException {
        eventDispatcher.addListener(hostReactor.getServiceInfo(NamingUtils.getGroupedName(serviceName, groupName),
            StringUtils.join(clusters, ",")), StringUtils.join(clusters, ","), listener);
    }

    @Override
    public void unsubscribe(String serviceName, EventListener listener) throws NacosException {
        unsubscribe(serviceName, new ArrayList<String>(), listener);
    }

    @Override
    public void unsubscribe(String serviceName, String groupName, EventListener listener) throws NacosException {
        unsubscribe(serviceName, groupName, new ArrayList<String>(), listener);
    }

    @Override
    public void unsubscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException {
        unsubscribe(serviceName, Constants.DEFAULT_GROUP, clusters, listener);
    }

    @Override
    public void unsubscribe(String serviceName, String groupName, List<String> clusters, EventListener listener) throws NacosException {
        eventDispatcher.removeListener(NamingUtils.getGroupedName(serviceName, groupName), StringUtils.join(clusters, ","), listener);
    }

    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize) throws NacosException {
        return serverProxy.getServiceList(pageNo, pageSize, Constants.DEFAULT_GROUP);
    }

    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName) throws NacosException {
        return getServicesOfServer(pageNo, pageSize, groupName, null);
    }

    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, AbstractSelector selector)
        throws NacosException {
        return getServicesOfServer(pageNo, pageSize, Constants.DEFAULT_GROUP, selector);
    }

    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName, AbstractSelector selector) throws NacosException {
        return serverProxy.getServiceList(pageNo, pageSize, groupName, selector);
    }

    @Override
    public List<ServiceInfo> getSubscribeServices() {
        return eventDispatcher.getSubscribeServices();
    }

    @Override
    public String getServerStatus() {
        return serverProxy.serverHealthy() ? "UP" : "DOWN";
    }

    private List<Instance> selectInstances(ServiceInfo serviceInfo, boolean healthy) {
        List<Instance> list;
        if (serviceInfo == null || CollectionUtils.isEmpty(list = serviceInfo.getHosts())) {
            return new ArrayList<Instance>();
        }

        Iterator<Instance> iterator = list.iterator();
        while (iterator.hasNext()) {
            Instance instance = iterator.next();
            if (healthy != instance.isHealthy() || !instance.isEnabled() || instance.getWeight() <= 0) {
                iterator.remove();
            }
        }

        return list;
    }

    public BeatReactor getBeatReactor() {
        return beatReactor;
    }
}
