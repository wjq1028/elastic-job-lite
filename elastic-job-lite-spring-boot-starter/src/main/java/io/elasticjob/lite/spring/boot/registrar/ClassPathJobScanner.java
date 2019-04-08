package io.elasticjob.lite.spring.boot.registrar;

import io.elasticjob.lite.spring.boot.annotation.ElasticJob;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.util.Set;

public class ClassPathJobScanner extends ClassPathBeanDefinitionScanner {


    public ClassPathJobScanner(BeanDefinitionRegistry registry) {
        super(registry,false);
    }

    protected void registerFilters() {
        this.addIncludeFilter(new AnnotationTypeFilter(ElasticJob.class));
    }


    @Override
    protected Set<BeanDefinitionHolder> doScan(String... basePackages) {
        return super.doScan(basePackages);
    }
}
