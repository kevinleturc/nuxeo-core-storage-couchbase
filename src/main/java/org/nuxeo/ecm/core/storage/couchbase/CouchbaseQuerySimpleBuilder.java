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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collector;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.AsPath;

/**
 * Simple query builder for a Couchbase query.
 *
 * @since 9.1
 */
class CouchbaseQuerySimpleBuilder {

    private AsPath selectFrom;

    private Expression where;

    private Integer limit;

    public CouchbaseQuerySimpleBuilder(String bucket) {
        selectFrom = Select.select(Expression.i(bucket) + ".*").from(Expression.i(bucket));
    }

    public CouchbaseQuerySimpleBuilder(String bucket, String... keys) {
        selectFrom = Select.select(Arrays.stream(keys).map(Expression::i).toArray(Expression[]::new))
                           .from(Expression.i(bucket));
    }

    public CouchbaseQuerySimpleBuilder eq(String key, Object value) {
        Expression eq = Expression.i(key);
        Serializable serializable = CouchbaseStateSerializer.serializeScalar(value);
        if (serializable instanceof Boolean) {
            eq = eq.eq(((Boolean) serializable).booleanValue());
        } else if (serializable instanceof Long) {
            eq = eq.eq(((Long) serializable).longValue());
        } else if (serializable instanceof JsonObject) {
            eq = eq.eq((JsonObject) serializable);
        } else {
            eq = eq.eq(Expression.s(serializable.toString()));
        }
        where = where == null ? eq : where.and(eq);
        return this;
    }

    public CouchbaseQuerySimpleBuilder notIn(String key, Set<String> value) {
        if (!value.isEmpty()) {
            Expression notIn = Expression.i(key).notIn(
                    value.stream().collect(Collector.of(JsonArray::create, JsonArray::add, (ja1, ja2) -> {
                        ja2.forEach(ja1::add);
                        return ja1;
                    })));
            where = where == null ? notIn : where.and(notIn);
        }
        return this;
    }

    public CouchbaseQuerySimpleBuilder limit(int limit) {
        this.limit = new Integer(limit);
        return this;
    }

    public N1qlQuery build() {
        Statement statement = selectFrom.where(where);
        if (limit != null) {
            statement = selectFrom.where(where).limit(limit.intValue());
        }
        return N1qlQuery.simple(statement, N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS));
    }

    public static CouchbaseQuerySimpleBuilder selectAll(String bucket) {
        return new CouchbaseQuerySimpleBuilder(bucket);
    }

    public static CouchbaseQuerySimpleBuilder select(String bucket, String... keys) {
        return new CouchbaseQuerySimpleBuilder(bucket, keys);
    }
}
