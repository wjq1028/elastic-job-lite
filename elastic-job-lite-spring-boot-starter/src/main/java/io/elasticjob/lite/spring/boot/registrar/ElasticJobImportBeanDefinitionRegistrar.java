package io.elasticjob.lite.spring.boot.registrar;

import io.elasticjob.lite.spring.boot.annotation.EnableElasticJob;
import io.elasticjob.lite.spring.boot.autoconfigure.ElasticJobAutoconfigure;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author wangjianqiao
 */
public class ElasticJobImportBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar {


    @Override
    public void registerBeanDefinitions(AnnotationMetadata annotationMetadata, BeanDefinitionRegistry beanDefinitionRegistry) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(ElasticJobAutoconfigure.class);
        beanDefinitionRegistry.registerBeanDefinition("elasticJobAutoconfigure",beanDefinitionBuilder.getBeanDefinition());

        ClassPathJobScanner classPathJobScanner = new ClassPathJobScanner(beanDefinitionRegistry);
        AnnotationAttributes annoAttrs = AnnotationAttributes.fromMap(annotationMetadata.getAnnotationAttributes(EnableElasticJob.class.getName()));

        String[] baseBasePackages = annoAttrs.getStringArray("value");

        classPathJobScanner.registerFilters();
        classPathJobScanner.doScan(baseBasePackages);
    }
}
