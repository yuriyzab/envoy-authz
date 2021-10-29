package org.example.authserver.service;

import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.example.authserver.config.UserRelationsConfig;
import org.example.authserver.entity.UserRelationEntity;
import org.example.authserver.repo.AclRepository;
import org.example.authserver.repo.pgsql.UserRelationRepository;
import org.example.authserver.service.model.RequestCache;
import org.example.authserver.service.zanzibar.Zanzibar;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class UserRelationCacheBuilder {

    private final static ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(2);

    private final UserRelationsConfig config;
    private final Zanzibar zanzibar;
    private final AclRepository aclRepository;
    private final UserRelationRepository userRelationRepository;
    private final CacheService cacheService;

    private final List<String> inProgressUsers = new CopyOnWriteArrayList<>();
    private final List<String> scheduledUsers = new CopyOnWriteArrayList<>();

    public UserRelationCacheBuilder(UserRelationsConfig config, AclRepository aclRepository, UserRelationRepository userRelationRepository, Zanzibar zanzibar, CacheService cacheService) {
        this.config = config;
        this.aclRepository = aclRepository;
        this.userRelationRepository = userRelationRepository;
        this.zanzibar = zanzibar;
        this.cacheService = cacheService;

        EXECUTOR.scheduleAtFixedRate(this::scheduledBuild, 0, config.getScheduledPeriodTime(), config.getScheduledPeriodTimeUnit());
    }

    public boolean isInProgress() {
        return !inProgressUsers.isEmpty();
    }

    public void firstTimeBuildAsync() {
        Executors.newSingleThreadExecutor().submit(this::firstTimeBuild);
    }

    public boolean firstTimeBuild() {
        boolean isFirstTime = userRelationRepository.count() == 0;
        if (!isFirstTime) {
            return false;
        }

        return buildAll();
    }

    public boolean buildAll() {
        if (!this.config.isEnabled()) {
            log.warn("User relations cache is not enabled.");
            return false;
        }

        if (isInProgress()) {
            log.warn("Build process is already in progress. Skip.");
            return false;
        }

        Set<String> namespaces = loadAllNamespaces();
        if (namespaces.isEmpty()) {
            log.warn("Unable to find namespaces. Skip building all cache.");
            return false;
        }

        Page<String> userPage = processUserPage(0, namespaces);
        if (userPage.getTotalElements() == 0) {
            log.warn("Unable to find users.");
            return false;
        }

        for (int i = 1; i < userPage.getTotalPages(); i++) {
            processUserPage(i, namespaces);
        }
        return true;
    }

    private Page<String> processUserPage(int page, Set<String> namespaces) {
        Stopwatch started = Stopwatch.createStarted();
        log.info("Processing user page {}, namespaces {} ...", page, namespaces.size());

        Page<String> userPage = aclRepository.findAllEndUsers(PageRequest.of(page, config.getPageSize()));
        log.info("Found end users, page: {}, pageSize: {}, totalPages: {}", userPage.getNumber(), config.getPageSize(), userPage.getTotalPages());
        if (userPage.isEmpty()) {
            return userPage;
        }

        Set<String> endUsers = userPage.toSet();
        inProgressUsers.addAll(endUsers);

        try {
            for (String endUser : endUsers) {
                buildUserRelations(endUser, namespaces);
            }
        } finally {
            inProgressUsers.clear();
        }

        log.info("User relations are built successfully for page {}, time: {}ms", userPage.getNumber(), started.elapsed(TimeUnit.MILLISECONDS));
        return userPage;
    }

    public boolean build(String user) {
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
        scheduledUsers.remove(user);
        return true;
    }

    private void buildUserRelations(String user) {
        Set<String> namespaces = loadAllNamespaces();
        if (namespaces.isEmpty()) {
            log.warn("Unable to find namespaces. Skip building cache for user: {}", user);
            return;
        }

        buildUserRelations(user, namespaces);
    }

    public void buildUserRelations(String user, Set<String> namespaces) {
        Optional<UserRelationEntity> entityOptional = createEntity(user, namespaces);
        if (entityOptional.isEmpty()) {
            log.trace("No user relations found, user: {}", user);
            return;
        }

        userRelationRepository.save(entityOptional.get());
    }

    public Optional<UserRelationEntity> createEntity(String user, Set<String> namespaces) {
        if (StringUtils.isBlank(user) || "*".equals(user)) {
            log.trace("Skip building cache for user: {}", user);
            return Optional.empty();
        }

        long maxAclUpdated = aclRepository.findMaxAclUpdatedByPrincipal(user);
        RequestCache requestCache = cacheService.prepareHighCardinalityCache(user);
        Set<String> relations = collectRelations(user, namespaces, requestCache);

        return Optional.of(UserRelationEntity.builder()
                .user(user)
                .relations(relations)
                .maxAclUpdated(maxAclUpdated)
                .build());
    }

    private Set<String> collectRelations(String user, Set<String> namespaces, RequestCache requestCache) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        Set<String> relations = new HashSet<>();

        Page<String> objectPage = processObjectPage(0);
        log.info("Zanzibar user {}, namespaces {}, objects {}...", user, namespaces.size(), objectPage.getTotalPages());

        processZanzibar(user, namespaces, requestCache, relations, objectPage);

        for (int i = 1; i < objectPage.getTotalPages(); i++) {
            processZanzibar(user, namespaces, requestCache, relations, processObjectPage(i));
        }

        int allRelationsSize = relations.size();
        relations.removeAll(requestCache.getPrincipalHighCardinalityCache().getOrDefault(user, new HashSet<>()));

        log.info("Zanzibar returned {} relations for user {} (all rel count: {})", relations.size(), user, allRelationsSize);
        log.info("Finished zanzibar user relations for user {}, time: {}", user, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return relations;
    }

    private void processZanzibar(String user, Set<String> namespaces, RequestCache requestCache, Set<String> relations, Page<String> objectPage) {
        for (String object : objectPage.toSet()) {
            //Stopwatch stopwatch = Stopwatch.createStarted();
            for (String namespace : namespaces) {
                relations.addAll(zanzibar.getRelations(namespace, object, user, requestCache));
            }
            //log.info("Gather relations for all namespaces {} and per user {}, object {}, {}ms", namespaces.size(), user, object, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    private void scheduledBuild() {
        if (scheduledUsers.isEmpty()) {
            return;
        }

        for (String user : new HashSet<>(scheduledUsers)) {
            build(user);
        }
    }

    public boolean fullRebuildAsync() {
        if (isInProgress()) {
            log.warn("Build process is already in progress. Skip.");
            return false;
        }

        EXECUTOR.execute(() -> {
            userRelationRepository.deleteAll();
            buildAll();
        });

        log.info("Scheduled full rebuild.");
        return true;
    }

    public boolean buildAsync(String user) {
        EXECUTOR.execute(() -> build(user));

        log.info("Scheduled updated for user {}.", user);
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

    private Page<String> processNamespacePage(int page) {
        Page<String> namespacePage = aclRepository.findAllNamespaces(PageRequest.of(page, config.getPageSize()));
        log.info("Found namespaces, page: {}, pageSize: {}, totalPages: {}", namespacePage.getNumber(), config.getPageSize(), namespacePage.getTotalPages());
        return namespacePage;
    }

    private Page<String> processObjectPage(int page) {
        Page<String> objectPage = aclRepository.findAllObjects(PageRequest.of(page, config.getPageSize()));
        //log.info("Found objects, page: {}, pageSize: {}, totalPages: {}", objectPage.getNumber(), config.getPageSize(), objectPage.getTotalPages());
        return objectPage;
    }

    private Set<String> loadAllNamespaces() {
        return loadAll(this::processNamespacePage);
    }

    public static Set<String> loadAll(Function<Integer, Page<String>> pageLoader) {
        Page<String> page = pageLoader.apply(0);
        Set<String> namespaces = new HashSet<>(page.toSet());

        for (int i = 1; i < page.getTotalPages(); i++) {
            namespaces.addAll(pageLoader.apply(i).toSet());
        }
        return namespaces;
    }
}
