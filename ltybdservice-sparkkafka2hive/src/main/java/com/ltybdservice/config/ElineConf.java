package com.ltybdservice.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

/**
 * @author: Administrator$
 * @project: ltybdservice-root$
 * @date: 2017/12/19$ 16:49$
 * @description:
 */
public class ElineConf {
    private String bootstrapServers;
    private String subscribeType;
    private String checkpointLocation;
    public static ElineConf param = new ElineConf().getParam();

    private synchronized ElineConf getParam() {
        if (param != null) {
            return param;
        }
        InputStream in = this.getClass().getResourceAsStream("/eline.yml");
        ElineConf kafkaConf = new Yaml().loadAs(in, ElineConf.class);
        return kafkaConf;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getSubscribeType() {
        return subscribeType;
    }

    public void setSubscribeType(String subscribeType) {
        this.subscribeType = subscribeType;
    }


    public String getCheckpointLocation() {
        return checkpointLocation;
    }

    public void setCheckpointLocation(String checkpointLocation) {
        this.checkpointLocation = checkpointLocation;
    }

}
