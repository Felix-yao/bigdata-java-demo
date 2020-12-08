package com.felix.hdfs.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @ClassName : HdfsConfig
 * @Description : hdfs配置类
 * @Author : felix
 * @Date: 2020-11-25 17:37
 */


@Configuration
public class HdfsConfig {
    @Value("${hdfs.path}")
    private String path;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}

