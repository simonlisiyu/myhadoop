package com.lsy.myhadoop.cloudera;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.v14.RootResourceV14;
import com.lsy.myhadoop.cloudera.cm.config.CmConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Created by lisiyu on 2019/12/5.
 */
@SpringBootApplication
@EnableConfigurationProperties
public class ClouderaApplication {

    private static final Logger logger = LoggerFactory.getLogger(ClouderaApplication.class);

    @Autowired
    private CmConfig config;

    @Bean
    public RootResourceV14 rootResource() {
        logger.info("cm addr:{}:{}",config.getAddress(),config.getPort());
        return new ClouderaManagerClientBuilder()
                .withHost(config.getAddress())
                .withPort(config.getPort())
                .withUsernamePassword(config.getUsername(),config.getPassword())
                .build()
                .getRootV14();
    }

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("tomcat.util.http.parser.HttpParser.requestTargetAllow", "{}");
        SpringApplication.run(ClouderaApplication.class, args);
    }

}
