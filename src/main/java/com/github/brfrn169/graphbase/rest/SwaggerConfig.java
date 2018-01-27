package com.github.brfrn169.graphbase.rest;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration @EnableSwagger2 public class SwaggerConfig {

    @Bean public Docket document() {
        return new Docket(DocumentationType.SWAGGER_2).select()
            .paths(PathSelectors.regex("/v1/graphs.*")).build().apiInfo(apiInfo());
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder().title("Graphbase Rest API").version("0.1").build();
    }
}
