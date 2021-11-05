package org.example.authserver.service;

import authserver.acl.Acl;
import com.google.common.base.Stopwatch;
import io.micrometer.core.annotation.Timed;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.example.authserver.config.UserRelationsConfig;
import org.example.authserver.entity.UserRelationEntity;
import org.example.authserver.repo.AclRepository;
import org.example.authserver.repo.pgsql.UserRelationRepository;
import org.example.authserver.service.model.RequestCache;
import org.example.authserver.service.zanzibar.AclRelationConfigService;
import org.example.authserver.service.zanzibar.Zanzibar;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class UserRelationCacheBuilder {

    private final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    private final UserRelationsConfig config;
    private final Zanzibar zanzibar;
    private final AclRepository aclRepository;
    private final UserRelationRepository userRelationRepository;
    private final CacheService cacheService;
    private final AclRelationConfigService aclRelationConfigService;

    private final List<String> inProgressUsers = new CopyOnWriteArrayList<>();
    private final List<String> scheduledUsers = new CopyOnWriteArrayList<>();

    public UserRelationCacheBuilder(UserRelationsConfig config, AclRepository aclRepository, UserRelationRepository userRelationRepository, Zanzibar zanzibar, CacheService cacheService, AclRelationConfigService aclRelationConfigService) {
        this.config = config;
        this.aclRepository = aclRepository;
        this.userRelationRepository = userRelationRepository;
        this.zanzibar = zanzibar;
        this.cacheService = cacheService;
        this.aclRelationConfigService = aclRelationConfigService;

        scheduler.scheduleAtFixedRate(this::scheduledBuild, 0, config.getScheduledPeriodTime(), config.getScheduledPeriodTimeUnit());
    }

    public boolean isInProgress() {
        return !inProgressUsers.isEmpty();
    }

    public boolean buildScheduled() {
        if (!this.config.isEnabled()) {
            log.warn("User relations cache is not enabled.");
            return false;
        }

        if (isInProgress()) {
            log.warn("Build process is already in progress. Skip.");
            return false;
        }

        Stopwatch started = Stopwatch.createStarted();
        log.info("Building all user relations...");

        Set<String> users = new HashSet<>(scheduledUsers);
        log.info("Found {} end users for building relations cache.", users.size());
        if (users.isEmpty()) {
            return false;
        }

        inProgressUsers.addAll(users);

        for (String user : users) {
            buildUserRelations(user);
            inProgressUsers.remove(user);
        }

        log.info("All user relations are built successfully. {}ms", started.elapsed(TimeUnit.MILLISECONDS));
        return true;
    }

    public boolean build(String user) {
        if (user == null) return false;
        if (!this.config.isEnabled() || !this.config.isUpdateOnAclChange()) {
            log.trace("User relations cache update is skipped. Enabled: {}, UpdateOnAclChange: {}", config.isEnabled(), config.isUpdateOnAclChange());
            return false;
        }

        if (inProgressUsers.contains(user)) {
            log.warn("Building for user {} is already in progress. Scheduled update for later.", user);
            scheduledUsers.add(user);
            return false;
        }

        inProgressUsers.add(user);
        buildUserRelations(user);
        inProgressUsers.remove(user);

        return true;
    }

    private boolean isUpToDate(String user, long maxAclUpdated) {
        Optional<UserRelationEntity> entityOptional = userRelationRepository.findById(user);
        if (entityOptional.isEmpty()) {
            return false;
        }
        return entityOptional.get().getMaxAclUpdated() >= maxAclUpdated;
    }

    private void buildUserRelations(String user) {
        long maxAclUpdated = aclRepository.findMaxAclUpdatedByPrincipal(user);
        if (isUpToDate(user, maxAclUpdated)) {
            log.debug("User's cache is already up-to-date. Skip building cache for user {}.", user);
            return;
        }

        Set<Acl> highCardinalityAcls = aclRepository.findAllByPrincipal(user);

        Set<Tuple2<String, String>> result = new HashSet<>();
        for (Acl acl : highCardinalityAcls){
            result.add(Tuples.of(acl.getNamespace(), acl.getObject()));
            Set<Tuple2<String, String>> lowCardinalityTuples = getLowCardinalityRelations(acl.getNamespace(), acl.getObject(), acl.getRelation(), new HashSet<>());
            result.addAll(lowCardinalityTuples);
        }

        buildUserRelations(user, result, maxAclUpdated);
    }

    private Set<Tuple2<String, String>> getLowCardinalityRelations(String usersetNamespace, String usersetObject, String relation, Set<Tuple2<String, String>> cache) {
        if (usersetNamespace == null || usersetObject == null || relation == null) return cache;

        log.info("Lookup for low cardinality: {}, {}, {}", usersetNamespace, usersetObject, relation);
        Set<String> relations = aclRelationConfigService.nestedRelations(usersetNamespace, usersetObject, relation);

        for (String rel : relations) {
            Set<Acl> acls = aclRepository.findAllForCache(usersetNamespace, usersetObject, rel);
            if (acls.size() == 0) continue;

            for (Acl acl : acls) {
                cache.add(Tuples.of(acl.getNamespace(), acl.getObject()));
                Set<Tuple2<String, String>> res = getLowCardinalityRelations(acl.getNamespace(), acl.getObject(), acl.getRelation(), cache);
                cache.addAll(res);
            }
        }

        return cache;
    }

    @Timed(value = "relation.cache.build", percentiles = {0.99, 0.95, 0.75})
    public void buildUserRelations(String user, Set<Tuple2<String, String>> nsObjects, long maxAclUpdated) {
        Optional<UserRelationEntity> entityOptional = createUserRelations(user, nsObjects, maxAclUpdated);
        if (entityOptional.isEmpty()) {
            log.trace("No user relations found, user: {}", user);
            return;
        }

        userRelationRepository.save(entityOptional.get());
    }

    public Optional<UserRelationEntity> createUserRelations(String user, Set<Tuple2<String, String>> nsObjects, long maxAclUpdated) {
        if (StringUtils.isBlank(user) || "*".equals(user)) {
            log.trace("Skip building cache for user: {}", user);
            return Optional.empty();
        }

        RequestCache requestCache = cacheService.prepareHighCardinalityCache(user);

        Stopwatch stopwatch = Stopwatch.createStarted();
        log.trace("Building user relations cache for user {} ...", user);

        Set<String> relations = new HashSet<>();
        for (Tuple2<String, String> nsObject : nsObjects) {
            relations.addAll(zanzibar.getRelations(nsObject.getT1(), nsObject.getT2(), user, requestCache));
        }

        int allRelationsSize = relations.size();

        log.trace("Found {} relations for user {}", relations.size(), user);
        log.trace("All rel count: {}, maxUpdated: {})", allRelationsSize, maxAclUpdated);
        log.debug("Finished building user relations cache for user {}, time: {}", user, stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return Optional.of(UserRelationEntity.builder()
                .user(user)
                .relations(relations)
                .maxAclUpdated(maxAclUpdated)
                .build());
    }

    private void scheduledBuild() {
        if (scheduledUsers.isEmpty()) {
            return;
        }

        for (String user : new HashSet<>(scheduledUsers)) {
            if (inProgressUsers.contains(user)) continue; // skip users in progress

            build(user);
            scheduledUsers.remove(user);
        }
    }

    public boolean updateScheduledAsync() {
        if (isInProgress()) {
            log.warn("Build process is already in progress. Skip.");
            return false;
        }

        scheduler.execute(this::buildScheduled);

        log.info("Scheduled full rebuild.");
        return true;
    }

    public boolean buildAsync(String user) {
        if (user == null) return false;
        if (!inProgressUsers.contains(user)) {
            scheduler.execute(() -> build(user));
            log.info("Scheduled updated for user {}.", user);
        }
        return true;
    }

    public boolean hasScheduled(String user) {
        return scheduledUsers.contains(user);
    }

    public boolean hasInProgress(String user) {
        return inProgressUsers.contains(user);
    }

    public boolean canUseCache(String user) {
        return config.isEnabled() && !hasScheduled(user) && !hasInProgress(user);
    }

    public boolean scheduleUpdate(String user) {
        if (user == null) return false;
        if (!hasScheduled(user)){
            scheduledUsers.add(user);
        }
        return true;
    }
}
