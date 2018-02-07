/*
 * (C) Copyright 2017 Nuxeo SA (http://nuxeo.com/) and others.
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.nuxeo.ecm.core.storage.dbs.DBSDocument.KEY_PRIMARY_TYPE;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.nuxeo.ecm.core.query.sql.NXQL;
import org.nuxeo.ecm.core.query.sql.model.Expression;
import org.nuxeo.ecm.core.query.sql.model.LiteralList;
import org.nuxeo.ecm.core.query.sql.model.Operator;
import org.nuxeo.ecm.core.query.sql.model.Reference;
import org.nuxeo.ecm.core.query.sql.model.SelectClause;
import org.nuxeo.ecm.core.query.sql.model.StringLiteral;
import org.nuxeo.ecm.core.storage.dbs.DBSExpressionEvaluator;
import org.nuxeo.ecm.core.storage.dbs.DBSSession;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.LocalDeploy;
import org.nuxeo.runtime.test.runner.RuntimeFeature;

import com.couchbase.client.java.query.Statement;

/**
 * @since 9.1
 */
@RunWith(FeaturesRunner.class)
@Features(RuntimeFeature.class)
@Deploy("org.nuxeo.ecm.core.schema")
@LocalDeploy("org.nuxeo.ecm.core.storage.couchbase.test:OSGI-INF/test-types-contrib.xml")
public class TestCouchbaseQueryBuilder {

    @Test
    public void testEqOperatorOnString() throws Exception {
        SelectClause selectClause = newSelectClause();

        Expression expression = new Expression(new Reference(NXQL.ECM_NAME), Operator.EQ,
                new StringLiteral("name"));

        DBSExpressionEvaluator evaluator = new DBSExpressionEvaluator(null, selectClause, expression, null, null,
                false);

        // Test
        Statement statement = new CouchbaseQueryBuilder(evaluator, null, false).build("bucket");
        assertFileAgainstString("query-expression/eq-operator-on-string.n1ql", statement.toString());
    }

    @Test
    public void testEqOperatorOnEcmPath() throws Exception {
        SelectClause selectClause = newSelectClause();

        Expression expression = new Expression(new Reference(NXQL.ECM_PATH), Operator.EQ,
                new StringLiteral("/default-domain"));

        // Mock session
        DBSSession session = mock(DBSSession.class, (Answer) invocation -> {
            if ("getDocumentIdByPath".equals(invocation.getMethod().getName())) {
                return "12345678-1234-1234-1234-123456789ABC";
            }
            return invocation.callRealMethod();
        });
        DBSExpressionEvaluator evaluator = new DBSExpressionEvaluator(session, selectClause, expression, null, null,
                false);

        // Test
        Statement statement = new CouchbaseQueryBuilder(evaluator, null, false).build("bucket");
        assertFileAgainstString("query-expression/eq-operator-on-ecm-path.n1ql", statement.toString());
    }

    @Test
    public void testAndOperator() throws Exception {
        SelectClause selectClause = newSelectClause();

        Expression nameExpression = new Expression(new Reference(NXQL.ECM_NAME), Operator.EQ,
                new StringLiteral("name"));
        Expression lockOwnerExpression = new Expression(new Reference(NXQL.ECM_LOCK_OWNER), Operator.EQ,
                new StringLiteral("bob"));
        Expression expression = new Expression(nameExpression, Operator.AND, lockOwnerExpression);

        DBSExpressionEvaluator evaluator = new DBSExpressionEvaluator(null, selectClause, expression, null, null,
                false);

        // Test
        Statement statement = new CouchbaseQueryBuilder(evaluator, null, false).build("bucket");
        assertFileAgainstString("query-expression/and-operator.n1ql", statement.toString());
    }

    @Test
    public void testOrOperator() throws Exception {
        SelectClause selectClause = newSelectClause();

        Expression nameExpression = new Expression(new Reference(NXQL.ECM_NAME), Operator.EQ,
                new StringLiteral("name"));
        Expression lockOwnerExpression = new Expression(new Reference(NXQL.ECM_LOCK_OWNER), Operator.EQ,
                new StringLiteral("bob"));
        Expression expression = new Expression(nameExpression, Operator.OR, lockOwnerExpression);

        DBSExpressionEvaluator evaluator = new DBSExpressionEvaluator(null, selectClause, expression, null, null,
                false);

        // Test
        Statement statement = new CouchbaseQueryBuilder(evaluator, null, false).build("bucket");
        assertFileAgainstString("query-expression/or-operator.n1ql", statement.toString());
    }

    @Test
    public void testIsNullOperator() throws Exception {
        SelectClause selectClause = newSelectClause();

        Expression expression = new Expression(new Reference(NXQL.ECM_LOCK_CREATED), Operator.ISNULL, null);

        DBSExpressionEvaluator evaluator = new DBSExpressionEvaluator(null, selectClause, expression, null, null,
                false);

        // Test
        Statement statement = new CouchbaseQueryBuilder(evaluator, null, false).build("bucket");
        assertFileAgainstString("query-expression/is-null-operator.n1ql", statement.toString());
    }


    @Test
    public void testInOperator() throws Exception {
        SelectClause selectClause = newSelectClause();

        LiteralList inPrimaryTypes = new LiteralList();
        inPrimaryTypes.add(new StringLiteral("Document"));
        inPrimaryTypes.add(new StringLiteral("Folder"));
        Expression expression = new Expression(new Reference(KEY_PRIMARY_TYPE), Operator.IN, inPrimaryTypes);
        DBSExpressionEvaluator evaluator = new DBSExpressionEvaluator(null, selectClause, expression, null, null,
                false);

        // Test
        Statement statement = new CouchbaseQueryBuilder(evaluator, null, false).build("bucket");
        assertFileAgainstString("query-expression/in-operator.n1ql", statement.toString());
    }

    public String readFile(String file) throws Exception {
        return new String(Files.readAllBytes(Paths.get(this.getClass().getResource("/" + file).toURI())));
    }

    public void assertFileAgainstString(String file, String actual) throws Exception {
        assertEquals(readFile(file), actual);
    }

    private SelectClause newSelectClause() {
        SelectClause selectClause = new SelectClause();
        selectClause.add(new Reference(NXQL.ECM_UUID));
        selectClause.add(new Reference(NXQL.ECM_NAME));
        selectClause.add(new Reference(NXQL.ECM_PARENTID));
        return selectClause;
    }

}
