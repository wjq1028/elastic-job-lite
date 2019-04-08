package io.elasticjob.lite.spring.boot.autoconfigure;


import io.elasticjob.lite.reg.zookeeper.ZookeeperConfiguration;
import io.elasticjob.lite.reg.zookeeper.ZookeeperRegistryCenter;
import io.elasticjob.lite.spring.boot.config.ElasticJobProperties;
import io.elasticjob.lite.spring.boot.parser.ElasticJobSpringBootParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * @author wangjianqiao
 */
@EnableConfigurationProperties(ElasticJobProperties.class)
@Slf4j
public class ElasticJobAutoconfigure {

    @Autowired
    ElasticJobProperties elasticJobProperties;

    /**
     * 初始化Zookeeper注册中心
     * @return
     */
    @Bean(name = "zookeeperRegistryCenter", value = "zookeeperRegistryCenter", initMethod = "init")
    @ConditionalOnMissingBean
    public ZookeeperRegistryCenter zookeeperRegistryCenter() {

        ElasticJobProperties.ZooKeeperProperties zooKeeperProperties = elasticJobProperties.getZooKeeper();
        ZookeeperConfiguration zookeeperConfiguration = new ZookeeperConfiguration(zooKeeperProperties.getServerLists(),
                zooKeeperProperties.getNamespace());
        zookeeperConfiguration.setBaseSleepTimeMilliseconds(zooKeeperProperties.getBaseSleepTimeMilliseconds());
        zookeeperConfiguration.setConnectionTimeoutMilliseconds(zooKeeperProperties.getConnectionTimeoutMilliseconds());
        zookeeperConfiguration.setDigest(zooKeeperProperties.getDigest());
        zookeeperConfiguration.setMaxRetries(zooKeeperProperties.getMaxRetries());
        zookeeperConfiguration.setMaxSleepTimeMilliseconds(zooKeeperProperties.getMaxSleepTimeMilliseconds());
        zookeeperConfiguration.setSessionTimeoutMilliseconds(zooKeeperProperties.getSessionTimeoutMilliseconds());
        log.debug("EJOB：初始化Zookeeper客户端,{}",zooKeeperProperties);
        return new ZookeeperRegistryCenter(zookeeperConfiguration);
    }

    @Bean
    @ConditionalOnMissingBean
    public ElasticJobSpringBootParser elasticJobSpringBootParser() {
        return new ElasticJobSpringBootParser();
    }


}
