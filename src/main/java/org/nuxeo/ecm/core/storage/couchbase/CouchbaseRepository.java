/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *  
 * Contributors:
 *     Kevin Leturc
 */
package org.nuxeo.ecm.core.storage.couchbase;

import static java.lang.Boolean.TRUE;
import static org.nuxeo.ecm.core.storage.dbs.DBSDocument.KEY_ID;
import static org.nuxeo.ecm.core.storage.dbs.DBSDocument.KEY_IS_PROXY;
import static org.nuxeo.ecm.core.storage.dbs.DBSDocument.KEY_NAME;
import static org.nuxeo.ecm.core.storage.dbs.DBSDocument.KEY_PARENT_ID;
import static org.nuxeo.ecm.core.storage.dbs.DBSDocument.KEY_PROXY_IDS;
import static org.nuxeo.ecm.core.storage.dbs.DBSDocument.KEY_PROXY_TARGET_ID;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.resource.spi.ConnectionManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.Lock;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.api.PartialList;
import org.nuxeo.ecm.core.api.ScrollResult;
import org.nuxeo.ecm.core.model.Repository;
import org.nuxeo.ecm.core.query.sql.model.OrderByClause;
import org.nuxeo.ecm.core.storage.State;
import org.nuxeo.ecm.core.storage.State.StateDiff;
import org.nuxeo.ecm.core.storage.dbs.DBSExpressionEvaluator;
import org.nuxeo.ecm.core.storage.dbs.DBSRepositoryBase;
import org.nuxeo.ecm.core.storage.dbs.DBSStateFlattener;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;

import rx.Observable;

/**
 * Couchbase implementation of a {@link Repository}.
 *
 * @since 9.1
 */
public class CouchbaseRepository extends DBSRepositoryBase {

    private static final Log log = LogFactory.getLog(CouchbaseRepository.class);

    public static final String BUCKET_DEFAULT = "nuxeo";

    protected Cluster cluster;

    protected Bucket bucket;

    /** Last value used from the in-memory sequence. Used by unit tests. */
    protected long sequenceLastValue;

    public CouchbaseRepository(ConnectionManager cm, CouchbaseRepositoryDescriptor descriptor) {
        super(cm, descriptor.name, descriptor);
        try {
            cluster = newCluster(descriptor);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        String bucketname = StringUtils.defaultIfBlank(descriptor.bucketname, BUCKET_DEFAULT);
        bucket = cluster.openBucket(bucketname);
        initRepository();
    }

    @Override
    public List<IdType> getAllowedIdTypes() {
        return Collections.singletonList(IdType.varchar);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        cluster.disconnect();
    }

    // used also by unit tests
    public static Cluster newCluster(CouchbaseRepositoryDescriptor descriptor) throws UnknownHostException {
        Cluster ret;
        String server = descriptor.server;
        if (StringUtils.isBlank(server)) {
            throw new NuxeoException("Missing <server> in Couchbase repository descriptor");
        }
        if (server.startsWith("couchbase://")) {
            // allow couchbase:// URI syntax for the server, to pass everything in one string
            ret = CouchbaseCluster.fromConnectionString(server);
        } else {
            ret = CouchbaseCluster.create(server);
        }
        if (log.isDebugEnabled()) {
            log.debug("Couchbase cluster initialized");
        }
        return ret;
    }

    protected void initRepository() {
        if (readState(getRootId()) == null) {
            initRoot();
        }
    }

    @Override
    protected void initBlobsPaths() {

    }

    @Override
    public String generateNewId() {
        if (DEBUG_UUIDS) {
            Long id = getNextSequenceId();
            return "UUID_" + id;
        }
        return UUID.randomUUID().toString();
    }

    // Used by unit tests
    protected synchronized Long getNextSequenceId() {
        sequenceLastValue++;
        return Long.valueOf(sequenceLastValue);
    }

    @Override
    public State readState(String id) {
        if (log.isTraceEnabled()) {
            log.trace("Couchbase: READ " + id);
        }
        JsonDocument jsonDocument = bucket.get(id);
        if (jsonDocument == null) {
            return null;
        }
        return CouchbaseStateDeserializer.deserialize(jsonDocument.content());
    }

    @Override
    public List<State> readStates(List<String> ids) {
        if (log.isTraceEnabled()) {
            log.trace("Couchbase: READ " + ids);
        }
        return Observable.from(ids)
                         .flatMap(id -> bucket.async().get(id))
                         .map(CouchbaseStateDeserializer::deserialize)
                         .toList()
                         .toBlocking()
                         .single();
    }

    @Override
    public void createState(State state) {
        if (log.isTraceEnabled()) {
            log.trace("Couchbase: CREATE " + getId(state) + ": " + state);
        }
        bucket.insert(JsonDocument.create(getId(state), CouchbaseStateSerializer.serialize(state))).cas();
    }

    @Override
    public void createStates(List<State> states) {
        if (log.isTraceEnabled()) {
            log.trace("Couchbase: CREATE ["
                    + states.stream().map(state -> state.get(KEY_ID).toString()).collect(Collectors.joining(", "))
                    + "]: " + states);
        }
        Observable.from(states)
                  .map(state -> JsonDocument.create(getId(state), CouchbaseStateSerializer.serialize(state)))
                  .flatMap(doc -> bucket.async().insert(doc))
                  .last()
                  .toBlocking()
                  .single();
    }

    @Override
    public void updateState(String id, StateDiff diff) {
        N1qlQuery query = CouchbaseUpdateQueryBuilder.query(bucket.name(), id, diff);
        if (log.isTraceEnabled()) {
            logQuery(query);
        }
        bucket.query(query);
    }

    @Override
    public void deleteStates(Set<String> ids) {
        if (log.isTraceEnabled()) {
            log.trace("Couchbase: DELETE " + ids);
        }
        Observable.from(ids).flatMap(id -> bucket.async().remove(id)).last().toBlocking().single();
    }

    @Override
    public State readChildState(String parentId, String name, Set<String> ignored) {
        N1qlQuery query = CouchbaseQuerySimpleBuilder.selectAll(bucket.name())
                                                     .eq(KEY_PARENT_ID, parentId)
                                                     .eq(KEY_NAME, name)
                                                     .notIn(KEY_ID, ignored)
                                                     .limit(1)
                                                     .build();
        return findOne(query);
    }

    @Override
    public boolean hasChild(String parentId, String name, Set<String> ignored) {
        N1qlQuery query = CouchbaseQuerySimpleBuilder.selectAll(bucket.name())
                                                     .eq(KEY_PARENT_ID, parentId)
                                                     .eq(KEY_NAME, name)
                                                     .notIn(KEY_ID, ignored)
                                                     .limit(1)
                                                     .build();
        return exist(query);
    }

    @Override
    public List<State> queryKeyValue(String key, Object value, Set<String> ignored) {
        N1qlQuery query = CouchbaseQuerySimpleBuilder.selectAll(bucket.name())
                                                     .eq(key, value)
                                                     .notIn(KEY_ID, ignored)
                                                     .build();
        return findAll(query);
    }

    @Override
    public List<State> queryKeyValue(String key1, Object value1, String key2, Object value2, Set<String> ignored) {
        N1qlQuery query = CouchbaseQuerySimpleBuilder.selectAll(bucket.name())
                                                     .eq(key1, value1)
                                                     .eq(key2, value2)
                                                     .notIn(KEY_ID, ignored)
                                                     .build();
        return findAll(query);
    }

    @Override
    public void queryKeyValueArray(String key, Object value, Set<String> ids, Map<String, String> proxyTargets,
            Map<String, Object[]> targetProxies) {
        N1qlQuery query = CouchbaseQuerySimpleBuilder.select(bucket.name(), KEY_ID, KEY_IS_PROXY, KEY_PROXY_TARGET_ID,
                KEY_PROXY_IDS).eq(key, value).build();
        for (State state : findAll(query)) {
            String id = (String) state.get(KEY_ID);
            ids.add(id);
            if (proxyTargets != null && TRUE.equals(state.get(KEY_IS_PROXY))) {
                String targetId = (String) state.get(KEY_PROXY_TARGET_ID);
                proxyTargets.put(id, targetId);
            }
            if (targetProxies != null) {
                Object[] proxyIds = (Object[]) state.get(KEY_PROXY_IDS);
                if (proxyIds != null) {
                    targetProxies.put(id, proxyIds);
                }
            }
        }
    }

    @Override
    public boolean queryKeyValuePresence(String key, String value, Set<String> ignored) {
        N1qlQuery query = CouchbaseQuerySimpleBuilder.selectAll(bucket.name())
                                                     .eq(key, value)
                                                     .notIn(KEY_ID, ignored)
                                                     .limit(1)
                                                     .build();
        return exist(query);
    }

    @Override
    public PartialList<Map<String, Serializable>> queryAndFetch(DBSExpressionEvaluator evaluator,
            OrderByClause orderByClause, boolean distinctDocuments, int limit, int offset, int countUpTo) {
        CouchbaseQueryBuilder builder = new CouchbaseQueryBuilder(evaluator, orderByClause, distinctDocuments);
        Statement statement = builder.build(bucket.name());
        // Don't do manual projection if there are no projection wildcards, as this brings no new
        // information and is costly. The only difference is several identical rows instead of one.
        boolean manualProjection = builder.doManualProjection();
        if (manualProjection) {
            // we'll do post-treatment to re-evaluate the query to get proper wildcard projections
            // so we need the full state from the database
            evaluator.parse();
        }
        // TODO limit and offset
        N1qlQuery query = N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS));
        if (log.isTraceEnabled()) {
            logQuery(query);
        }

        Function<State, Stream<Map<String, Serializable>>> projectionFct = state -> Stream.of(
                DBSStateFlattener.flatten(state));
        if (manualProjection) {
            projectionFct = state -> evaluator.matches(state).stream();
        }
        N1qlQueryResult result = bucket.query(query);
        List<Map<String, Serializable>> projections = result.allRows()
                                                            .stream()
                                                            .map(N1qlQueryRow::value)
                                                            .map(CouchbaseStateDeserializer::deserialize)
                                                            .flatMap(projectionFct)
                                                            .collect(Collectors.toList());
        long totalSize;
        if (countUpTo == -1) {
            // count full size
            if (limit == 0) {
                totalSize = projections.size();
            } else {
                totalSize = result.info().sortCount();
            }
        } else if (countUpTo == 0) {
            // no count
            totalSize = -1; // not counted
        } else {
            // count only if less than countUpTo
            if (limit == 0) {
                totalSize = projections.size();
            } else {
                totalSize = result.info().sortCount();
            }
            if (totalSize > countUpTo) {
                totalSize = -2; // truncated
            }
        }

        if (log.isTraceEnabled() && projections.size() != 0) {
            log.trace("Couchbase:    -> " + projections.size());
        }
        return new PartialList<>(projections, totalSize);
    }

    @Override
    public ScrollResult scroll(DBSExpressionEvaluator evaluator, int batchSize, int keepAliveSeconds) {
        return null;
    }

    @Override
    public ScrollResult scroll(String scrollId) {
        return null;
    }

    @Override
    public Lock getLock(String id) {
        return null;
    }

    @Override
    public Lock setLock(String id, Lock lock) {
        return null;
    }

    @Override
    public Lock removeLock(String id, String owner) {
        return null;
    }

    @Override
    public void closeLockManager() {

    }

    @Override
    public void clearLockManagerCaches() {

    }

    @Override
    public void markReferencedBinaries() {

    }

    private String getId(State state) {
        return state.get(KEY_ID).toString();
    }

    private void logQuery(N1qlQuery query) {
        log.trace("Couchbase: " + query.statement());
    }

    private boolean exist(N1qlQuery query) {
        if (log.isTraceEnabled()) {
            logQuery(query);
        }
        return !bucket.query(query).allRows().isEmpty();
    }

    private State findOne(N1qlQuery query) {
        if (log.isTraceEnabled()) {
            logQuery(query);
        }
        return bucket.query(query)
                     .allRows()
                     .stream()
                     .map(N1qlQueryRow::value)
                     .map(CouchbaseStateDeserializer::deserialize)
                     .findFirst()
                     .orElse(null);
    }

    private List<State> findAll(N1qlQuery query) {
        if (log.isTraceEnabled()) {
            logQuery(query);
        }
        return bucket.query(query)
                     .allRows()
                     .stream()
                     .map(N1qlQueryRow::value)
                     .map(CouchbaseStateDeserializer::deserialize)
                     .collect(Collectors.toList());
    }

}
