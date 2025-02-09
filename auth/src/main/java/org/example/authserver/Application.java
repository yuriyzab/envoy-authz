package org.example.authserver;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.example.authserver.config.AppProperties;
import org.example.authserver.service.zanzibar.AclFilterService;
import org.example.authserver.service.AuthService;
import org.example.authserver.service.CacheLoaderService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import javax.annotation.PostConstruct;

@Slf4j
@EnableConfigurationProperties
@ConfigurationPropertiesScan
@SpringBootApplication
public class Application {

    private final AclFilterService aclFilterService;
    private final CacheLoaderService cacheLoaderService;
    private final AppProperties appProperties;
    private final int grpcPort;

    public Application(AclFilterService aclFilterService, CacheLoaderService cacheLoaderService, AppProperties appProperties, @Value("${grpc.port:8080}") int grpcPort) {
        this.aclFilterService = aclFilterService;
        this.cacheLoaderService = cacheLoaderService;
        this.appProperties = appProperties;
        this.grpcPort = grpcPort;
    }

    @PostConstruct
    public void start() throws Exception {
        cacheLoaderService.subscribe();

        Server server = ServerBuilder.forPort(grpcPort)
                .addService(new AuthService(aclFilterService, appProperties))
                .build();

        server.start();
        log.info("Started. Listen post: {}}", grpcPort);
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
