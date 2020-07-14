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
package com.alibaba.nacos.naming.cluster.servers;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;

/**
 * Member node of Nacos cluster
 *
 * server的基础信息
 *
 * 内部排序 根据 ip:port
 *
 * @author nkorange
 * @since 1.0.0
 */
public class Server implements Comparable<Server> {

    /**
     * IP of member
     */
    private String ip;

    /**
     * serving port of member.
     */
    private int servePort;

    /**
     * 默认值是UNKNOWN_SITE = "unknown"，标识该该server所处的cluster group的名字
     *
     */
    private String site = UtilsAndCommons.UNKNOWN_SITE;

    /**
     * 自动server的权重值，默认值是1
     */
    private int weight = 1;

    /**
     * additional weight, used to adjust manually
     *
     * 手工干预的额外权重，默认是0，如果不是0，在执行server权重时，会将adWeight + weight ，即加起来
     */
    private int adWeight;

    /**
     * 标识server是否alive的状态，默认是false,需要在确认alive的时候置为true
     */
    private boolean alive = false;

    /**
     * 上一次使用的时间戳，用在比较当前时间与上次使用的差值，是否超过了阈值distroServerExpiredMillis
     */
    private long lastRefTime = 0L;

    /**
     * 时间戳lastRefTime对应的字符串格式，其样式是yyyy-MM-dd HH:mm:ss
     */
    private String lastRefTimeStr;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getServePort() {
        return servePort;
    }

    public void setServePort(int servePort) {
        this.servePort = servePort;
    }

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public int getAdWeight() {
        return adWeight;
    }

    public void setAdWeight(int adWeight) {
        this.adWeight = adWeight;
    }

    public boolean isAlive() {
        return alive;
    }

    public void setAlive(boolean alive) {
        this.alive = alive;
    }

    public long getLastRefTime() {
        return lastRefTime;
    }

    public void setLastRefTime(long lastRefTime) {
        this.lastRefTime = lastRefTime;
    }

    public String getLastRefTimeStr() {
        return lastRefTimeStr;
    }

    public void setLastRefTimeStr(String lastRefTimeStr) {
        this.lastRefTimeStr = lastRefTimeStr;
    }

    public String getKey() {
        return ip + UtilsAndCommons.IP_PORT_SPLITER + servePort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Server server = (Server) o;
        return servePort == server.servePort && ip.equals(server.ip);
    }

    @Override
    public int hashCode() {
        int result = ip.hashCode();
        result = 31 * result + servePort;
        return result;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    @Override
    public int compareTo(Server server) {
        if (server == null) {
            return 1;
        }
        return this.getKey().compareTo(server.getKey());
    }
}
