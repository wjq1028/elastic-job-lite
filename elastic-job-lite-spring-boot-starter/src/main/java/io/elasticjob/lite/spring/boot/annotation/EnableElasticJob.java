package io.elasticjob.lite.spring.boot.annotation;

import io.elasticjob.lite.spring.boot.registrar.ElasticJobImportBeanDefinitionRegistrar;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Import({ElasticJobImportBeanDefinitionRegistrar.class})
public @interface EnableElasticJob {

    @AliasFor("jobBasePackages")
    String[] value() default {};

    @AliasFor("value")
    String[] jobBasePackages() default {};
}
