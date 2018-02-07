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

import java.net.UnknownHostException;

import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.storage.dbs.DBSHelper;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.query.Delete;
import com.couchbase.client.java.query.N1qlQuery;

public class DBSHelperImpl implements DBSHelper {

    private static final String SERVER_PROPERTY = "nuxeo.test.couchbase.server";

    private static final String BUCKETNAME_PROPERTY = "nuxeo.test.couchbase.bucketname";

    private static final String DEFAULT_SERVER = "127.0.0.1";

    private static final String DEFAULT_BUCKETNAME = "unittests";

    @Override
    public void init() {
        String server = defaultProperty(SERVER_PROPERTY, DEFAULT_SERVER);
        String bucketname = defaultProperty(BUCKETNAME_PROPERTY, DEFAULT_BUCKETNAME);
        CouchbaseRepositoryDescriptor descriptor = new CouchbaseRepositoryDescriptor();
        descriptor.name = "test";
        descriptor.server = server;
        descriptor.bucketname = bucketname;
        try {
            Cluster cluster = CouchbaseRepository.newCluster(descriptor);
            Bucket bucket = cluster.openBucket(bucketname);
            bucket.query(N1qlQuery.simple(Delete.deleteFromCurrentBucket()));
//             cluster.openBucket(bucketname).bucketManager().flush();
            cluster.disconnect();
        } catch (UnknownHostException e) {
            throw new NuxeoException("Couchbase cleaning failed", e);
        }
    }

}
