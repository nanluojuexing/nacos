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
package com.alibaba.nacos.naming.cluster;

import com.alibaba.nacos.naming.consistency.ConsistencyService;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * Detect and control the working status of local server
 *
 * nacos naming server中状态管理类。在启动时会执行executorService的定时调度器，每5ms执行一次调度，刷新serverStatus的值
 *
 * @author nkorange
 * @since 1.0.0
 */
@Service
public class ServerStatusManager {

    @Resource(name = "consistencyDelegate")
    private ConsistencyService consistencyService;

    @Autowired
    private SwitchDomain switchDomain;

    private ServerStatus serverStatus = ServerStatus.STARTING;

    @PostConstruct
    public void init() {
        GlobalExecutor.registerServerStatusUpdater(new ServerStatusUpdater());
    }

    private void refreshServerStatus() {
        // 首先判断overriddenServerStatus字段是否是blank，如果不空，说明最近已经被设置过状态了
        // 只需要将overriddenServerStatus赋值给serverStatus即可。如果是空，则需要通过判断当前的一致性服务是否可用来决定
        if (StringUtils.isNotBlank(switchDomain.getOverriddenServerStatus())) {
            serverStatus = ServerStatus.valueOf(switchDomain.getOverriddenServerStatus());
            return;
        }

        if (consistencyService.isAvailable()) {
            serverStatus = ServerStatus.UP;
        } else {
            serverStatus = ServerStatus.DOWN;
        }
    }

    public ServerStatus getServerStatus() {
        return serverStatus;
    }

    public class ServerStatusUpdater implements Runnable {

        @Override
        public void run() {
            refreshServerStatus();
        }
    }
}
