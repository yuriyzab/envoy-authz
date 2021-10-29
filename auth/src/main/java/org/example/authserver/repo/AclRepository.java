package org.example.authserver.repo;

import authserver.acl.Acl;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Set;

public interface AclRepository {

    Set<Acl> findAll();

    Acl findOneById(String id);

    Set<Acl> findAllByNamespaceAndObjectAndUser(String namespace, String object, String user);

    void save(Acl acl);

    void delete(Acl acl);

    Set<Acl> findAllByPrincipalAndNsObjectIn(String principal, List<String> nsObjects);
    Set<Acl> findAllByPrincipal(String principal);
    Set<Acl> findAllByNsObjectIn(List<String> nsObjects);

    Page<String> findAllEndUsers(Pageable pageable);

    Page<String> findAllNamespaces(Pageable pageable);

    Page<String> findAllObjects(Pageable pageable);

    long findMaxAclUpdatedByPrincipal(String principal);
}
