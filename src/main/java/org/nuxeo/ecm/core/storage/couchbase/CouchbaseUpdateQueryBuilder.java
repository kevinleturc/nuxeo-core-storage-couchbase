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

import static org.nuxeo.ecm.core.storage.dbs.DBSDocument.KEY_ID;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Calendar;

import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.storage.State.ListDiff;
import org.nuxeo.ecm.core.storage.State.StateDiff;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.Update;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.UpdateSetPath;
import com.couchbase.client.java.query.dsl.path.UpdateUsePath;

/**
 * Couchbase update query builder.
 *
 * @since 9.1
 */
class CouchbaseUpdateQueryBuilder {

    private UpdateUsePath useQuery;

    private UpdateSetPath setQuery;

    private CouchbaseUpdateQueryBuilder(String bucket) {
        useQuery = Update.update(Expression.i(bucket));
    }

    private void set(String key, Serializable value) {
        if (value instanceof Boolean) {
            set(key, (Boolean) value);
        } else if (value instanceof Long) {
            set(key, (Long) value);
        } else if (value instanceof String) {
            set(key, (String) value);
        } else if (value instanceof Calendar) {
            set(key, CouchbaseHelper.serializeCalendar((Calendar) value));
        } else if (value instanceof StateDiff) {
            ((StateDiff) value).entrySet().forEach(entry -> set(key + '.' + entry.getKey(), entry.getValue()));
        } else if (value instanceof ListDiff) {
            throw new UnsupportedOperationException("ListDiff are not supported.");
        } else if (value instanceof Object[]) {
            JsonArray array = CouchbaseStateSerializer.serializeList(Arrays.asList((Object[]) value));
            set(key, array);
        }
    }

    public void set(String key, Boolean value) {
        if (setQuery == null) {
            setQuery = useQuery.set(Expression.i(key), value.booleanValue());
        } else {
            setQuery = setQuery.set(Expression.i(key), value.booleanValue());
        }
    }

    public void set(String key, Long value) {
        if (setQuery == null) {
            setQuery = useQuery.set(Expression.i(key), value.longValue());
        } else {
            setQuery = setQuery.set(Expression.i(key), value.longValue());
        }
    }

    public void set(String key, String value) {
        if (setQuery == null) {
            setQuery = useQuery.set(Expression.i(key), value);
        } else {
            setQuery = setQuery.set(Expression.i(key), value);
        }
    }

    public void set(String key, JsonObject value) {
        if (setQuery == null) {
            setQuery = useQuery.set(Expression.i(key), value);
        } else {
            setQuery = setQuery.set(Expression.i(key), value);
        }
    }

    public void set(String key, JsonArray value) {
        if (setQuery == null) {
            setQuery = useQuery.set(Expression.i(key), value);
        } else {
            setQuery = setQuery.set(Expression.i(key), value);
        }
    }

    public N1qlQuery build(String id) {
        if (setQuery == null) {
            throw new NuxeoException("No set clause in update query.");
        }
        return N1qlQuery.simple(setQuery.where(Expression.i(KEY_ID).eq(Expression.s(id))),
                N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS));
    }

    public static N1qlQuery query(String bucket, String id, StateDiff diff) {
        CouchbaseUpdateQueryBuilder builder = new CouchbaseUpdateQueryBuilder(bucket);
        diff.entrySet().forEach(entry -> builder.set(entry.getKey(), entry.getValue()));
        return builder.build(id);
    }

}
