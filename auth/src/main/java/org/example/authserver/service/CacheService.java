package org.example.authserver.service;


import authserver.acl.AclRelationConfig;
import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class CacheService {

    private static final Map<String, AclRelationConfig> configs = new ConcurrentHashMap<>();

    public void updateConfigs(Map<String, AclRelationConfig> configMap) {
        configs.keySet().removeIf(cfg -> !configMap.containsKey(cfg));
        configs.putAll(configMap);
    }

    public void updateConfig(AclRelationConfig config) {
        configs.put(config.getNamespace(), config);
    }

    public void deleteConfig(String namespace) {
        configs.remove(namespace);
    }

    public Map<String, AclRelationConfig> getConfigs() {
        return configs;
    }

    public void prepareHighCardinalityCache(RequestCache requestCache, String user) {
        if (requestCache.getPrincipalHighCardinalityCache().containsKey(user) || "*".equals(user)) {
            return;
        }

        Set<Acl> acls = aclRepository.findAllByPrincipal(user);
        Set<String> highCardinalityRelations = acls.stream().map(Acl::getTag).collect(Collectors.toSet());
        requestCache.getPrincipalHighCardinalityCache().put(user, highCardinalityRelations);
    }
}
