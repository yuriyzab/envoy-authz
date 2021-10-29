package org.example.authserver.repo.pgsql;

import authserver.acl.Acl;
import lombok.extern.slf4j.Slf4j;
import org.example.authserver.entity.AclEntity;
import org.example.authserver.repo.AclRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Configuration
@ConditionalOnProperty(
        value="app.database",
        havingValue = "POSTGRES"
)
public class AclPgRepository implements AclRepository {

    private final AclSpringDataRepository repository;

    public AclPgRepository(AclSpringDataRepository repository) {
        this.repository = repository;
    }

    @Override
    public Set<Acl> findAll() {
        return repository.findAll().stream()
                .map(AclEntity::toAcl)
                .collect(Collectors.toSet());
    }

    @Override
    public Acl findOneById(String id) {
        return repository.findById(id)
                .map(AclEntity::toAcl)
                .orElse(null);
    }

    @Override
    public Set<Acl> findAllByPrincipalAndNsObjectIn(String principal, List<String> nsObjects) {
        Set<AclEntity> usersetAcls = repository.findAllByNsobjectInAndUser(nsObjects, "*");
        Set<AclEntity> userAcls = repository.findAllByUser(principal);
        return Stream.concat(usersetAcls.stream(), userAcls.stream())
                .map(AclEntity::toAcl)
                .collect(Collectors.toSet());
    }


    @Override
    public Set<Acl> findAllByPrincipal(String principal) {
        Set<AclEntity> userAcls = repository.findAllByUser(principal);
        return userAcls.stream()
                .map(AclEntity::toAcl)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Acl> findAllByNsObjectIn(List<String> nsObjects) {
        Set<AclEntity> usersetAcls = repository.findAllByNsobjectInAndUser(nsObjects, "*");
        return usersetAcls.stream()
                .map(AclEntity::toAcl)
                .collect(Collectors.toSet());
    }

    @Override
    public Page<String> findAllEndUsers(Pageable pageable) {
        return repository.findDistinctEndUsers(pageable);
    }

    @Override
    public Page<String> findAllNamespaces(Pageable pageable) {
        return repository.findDistinctNamespaces(pageable);
    }

    @Override
    public Page<String> findAllObjects(Pageable pageable) {
        return repository.findDistinctObjects(pageable);
    }

    @Override
    public long findMaxAclUpdatedByPrincipal(String principal) {
        return repository.findMaxAclUpdatedByPrincipal(principal);
    }

    @Override
    public Set<Acl> findAllByNamespaceAndObjectAndUser(String namespace, String object, String user) {
        Set<AclEntity> usersetAcls = repository.findAllByNsobjectAndUser(String.format("%s:%s", namespace, object), "*");
        Set<AclEntity> userAcls = repository.findAllByNsobjectAndUser(String.format("%s:%s", namespace, object), user);
        return Stream.concat(usersetAcls.stream(), userAcls.stream())
                .map(AclEntity::toAcl)
                .collect(Collectors.toSet());
    }


    @Override
    public void save(Acl acl) {
        AclEntity entity = AclEntity.builder()
                .id(acl.getId().toString())
                .nsobject(String.format("%s:%s", acl.getNamespace(), acl.getObject()))
                .namespace(acl.getNamespace())
                .object(acl.getObject())
                .relation(acl.getRelation())
                .user((acl.hasUserset()) ? "*" : acl.getUser())
                .usersetNamespace(acl.getUsersetNamespace())
                .usersetObject(acl.getUsersetObject())
                .usersetRelation(acl.getUsersetRelation())
                .build();

        repository.save(entity);
    }

    @Override
    public void delete(Acl acl) {
        repository.deleteById(acl.getId().toString());
    }


}
