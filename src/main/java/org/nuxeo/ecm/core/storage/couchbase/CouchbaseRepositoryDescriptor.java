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

import org.nuxeo.common.xmap.annotation.XNode;
import org.nuxeo.common.xmap.annotation.XObject;
import org.nuxeo.ecm.core.storage.dbs.DBSRepositoryDescriptor;

/**
 * Couchbase Repository Descriptor.
 *
 * @since 9.1
 */
@XObject(value = "repository")
public class CouchbaseRepositoryDescriptor extends DBSRepositoryDescriptor {

    public CouchbaseRepositoryDescriptor() {
    }

    @XNode("server")
    public String server;

    @XNode("bucketname")
    public String bucketname;

    public void merge(CouchbaseRepositoryDescriptor other) {
        super.merge(other);
        if (other.server != null) {
            server = other.server;
        }
        if (other.bucketname != null) {
            bucketname = other.bucketname;
        }
    }

}
