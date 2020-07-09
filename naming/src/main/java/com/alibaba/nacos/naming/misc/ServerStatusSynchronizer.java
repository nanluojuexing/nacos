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
package com.alibaba.nacos.naming.misc;

import com.alibaba.nacos.naming.boot.RunningConfig;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.springframework.util.StringUtils;

import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

/**
 * Report local server status to other server
 *
 * 主要实现对server的status相关操作，仔细看可以发现，仅有send的方法实现
 * 同样也是走HttpClient的asyncHttpGet方法来直接实现异步http的远程调用，并没有经过代理，其中path中有一节是/operator，说明是由OperatorController来处理
 *
 * @author nacos
 */
public class ServerStatusSynchronizer implements Synchronizer {

    /**
     * asyncHttpGet方法发送出去的，也就是异步http的get方法，
     * 其发送path是RunningConfig.getContextPath() + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/operator/server/status"，
     * 其中RunningConfig.getContextPath()默认是/nacos,即/nacos/v1/ns/operator/server/status

     * @param serverIP target server address
     * @param msg      message to send
     */
    @Override
    public void send(final String serverIP, Message msg) {
        if (StringUtils.isEmpty(serverIP)) {
            return;
        }

        final Map<String, String> params = new HashMap<String, String>(2);

        params.put("serverStatus", msg.getData());

        String url = "http://" + serverIP + ":" + RunningConfig.getServerPort()
            + RunningConfig.getContextPath() + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/operator/server/status";

        if (serverIP.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
            url = "http://" + serverIP + RunningConfig.getContextPath() + UtilsAndCommons.NACOS_NAMING_CONTEXT
                + "/operator/server/status";
        }

        try {
            HttpClient.asyncHttpGet(url, null, params, new AsyncCompletionHandler() {
                @Override
                public Integer onCompleted(Response response) throws Exception {
                    if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                        Loggers.SRV_LOG.warn("[STATUS-SYNCHRONIZE] failed to request serverStatus, remote server: {}",
                            serverIP);

                        return 1;
                    }
                    return 0;
                }
            });
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("[STATUS-SYNCHRONIZE] failed to request serverStatus, remote server: {}", serverIP, e);
        }
    }

    @Override
    public Message get(String server, String key) {
        return null;
    }
}
