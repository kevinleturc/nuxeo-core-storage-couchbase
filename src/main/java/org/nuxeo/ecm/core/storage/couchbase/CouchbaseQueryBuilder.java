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

import static org.nuxeo.ecm.core.storage.dbs.DBSDocument.KEY_MIXIN_TYPES;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.nuxeo.ecm.core.query.QueryParseException;
import org.nuxeo.ecm.core.query.sql.NXQL;
import org.nuxeo.ecm.core.query.sql.model.BooleanLiteral;
import org.nuxeo.ecm.core.query.sql.model.DateLiteral;
import org.nuxeo.ecm.core.query.sql.model.DoubleLiteral;
import org.nuxeo.ecm.core.query.sql.model.IntegerLiteral;
import org.nuxeo.ecm.core.query.sql.model.Literal;
import org.nuxeo.ecm.core.query.sql.model.LiteralList;
import org.nuxeo.ecm.core.query.sql.model.MultiExpression;
import org.nuxeo.ecm.core.query.sql.model.Operand;
import org.nuxeo.ecm.core.query.sql.model.Operator;
import org.nuxeo.ecm.core.query.sql.model.OrderByClause;
import org.nuxeo.ecm.core.query.sql.model.Reference;
import org.nuxeo.ecm.core.query.sql.model.SelectClause;
import org.nuxeo.ecm.core.query.sql.model.StringLiteral;
import org.nuxeo.ecm.core.schema.SchemaManager;
import org.nuxeo.ecm.core.schema.types.ComplexType;
import org.nuxeo.ecm.core.schema.types.Field;
import org.nuxeo.ecm.core.schema.types.ListType;
import org.nuxeo.ecm.core.schema.types.Schema;
import org.nuxeo.ecm.core.schema.types.Type;
import org.nuxeo.ecm.core.schema.types.primitives.BooleanType;
import org.nuxeo.ecm.core.schema.types.primitives.DateType;
import org.nuxeo.ecm.core.storage.ExpressionEvaluator.PathResolver;
import org.nuxeo.ecm.core.storage.dbs.DBSExpressionEvaluator;
import org.nuxeo.ecm.core.storage.dbs.DBSSession;
import org.nuxeo.runtime.api.Framework;

import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;

/**
 * Couchbase query builder.
 *
 * @since 9.1
 */
class CouchbaseQueryBuilder {

    private static final Long ZERO = 0L;

    private static final Long ONE = 1L;

    private static final String DATE_CAST = "DATE";

    // non-canonical index syntax, for replaceAll
    private final static Pattern NON_CANON_INDEX = Pattern.compile("[^/\\[\\]]+" // name
            + "\\[(\\d+|\\*|\\*\\d+)\\]" // index in brackets
    );

    /** Splits foo/*1/bar into foo/*1, bar with the last bar part optional */
    private final static Pattern WILDCARD_SPLIT = Pattern.compile("(.*/\\*\\d+)(?:/(.*))?");

    private final SchemaManager schemaManager;

    private final org.nuxeo.ecm.core.query.sql.model.Expression expression;

    private final SelectClause selectClause;

    private final OrderByClause orderByClause;

    private final Set<String> principals;

    private final PathResolver pathResolver;

    private final boolean fulltextSearchDisabled;

    private final boolean distinctDocuments;

    private Boolean projectionHasWildcard;

    public CouchbaseQueryBuilder(DBSExpressionEvaluator evaluator, OrderByClause orderByClause,
            boolean distinctDocuments) {
        this.schemaManager = Framework.getService(SchemaManager.class);
        this.expression = evaluator.getExpression();
        this.selectClause = evaluator.getSelectClause();
        this.orderByClause = orderByClause;
        this.principals = evaluator.principals;
        this.pathResolver = evaluator.pathResolver;
        this.fulltextSearchDisabled = evaluator.fulltextSearchDisabled;
        this.distinctDocuments = distinctDocuments;
    }

    public boolean doManualProjection() {
        // Don't do manual projection if there are no projection wildcards, as this brings no new
        // information and is costly. The only difference is several identical rows instead of one.
        return !distinctDocuments && hasProjectionWildcard();
    }

    private boolean hasProjectionWildcard() {
        if (projectionHasWildcard == null) {
            projectionHasWildcard = Boolean.FALSE;
            for (Operand op : selectClause.getSelectList().values()) {
                if (!(op instanceof Reference)) {
                    throw new QueryParseException("Projection not supported: " + op);
                }
                if (walkReference(op).hasWildcard()) {
                    projectionHasWildcard = Boolean.TRUE;
                    break;
                }
            }
        }
        return projectionHasWildcard.booleanValue();
    }

    public Statement build(String bucket) {
        Expression query = walkExpression(expression).build();

        Statement statement = Select.select(Expression.i(bucket) + ".*").from(Expression.i(bucket)).where(query);
        // if (limit != null) {
        // statement = selectFrom.where(where).limit(limit.intValue());
        // }
        return statement;
    }

    private QueryBuilder walkExpression(org.nuxeo.ecm.core.query.sql.model.Expression expression) {
        Operator op = expression.operator;
        Operand lvalue = expression.lvalue;
        Operand rvalue = expression.rvalue;
        Reference ref = lvalue instanceof Reference ? (Reference) lvalue : null;
        String name = ref != null ? ref.name : null;
        String cast = ref != null ? ref.cast : null;
        if (DATE_CAST.equals(cast)) {
            checkDateLiteralForCast(op, rvalue, name);
        }

        if (op == Operator.STARTSWITH) {
            // return walkStartsWith(lvalue, rvalue);
        } else if (NXQL.ECM_PATH.equals(name)) {
            // return walkEcmPath(op, rvalue);
        } else if (name != null && name.startsWith(NXQL.ECM_FULLTEXT) && !NXQL.ECM_FULLTEXT_JOBID.equals(name)) {
            // return walkEcmFulltext(name, op, rvalue);
        } else if (op == Operator.SUM) {
            throw new UnsupportedOperationException("SUM");
        } else if (op == Operator.SUB) {
            throw new UnsupportedOperationException("SUB");
        } else if (op == Operator.MUL) {
            throw new UnsupportedOperationException("MUL");
        } else if (op == Operator.DIV) {
            throw new UnsupportedOperationException("DIV");
        } else if (op == Operator.LT) {
            // return walkLt(lvalue, rvalue);
        } else if (op == Operator.GT) {
            // return walkGt(lvalue, rvalue);
        } else if (op == Operator.EQ) {
            return walkEq(lvalue, rvalue, true);
        } else if (op == Operator.NOTEQ) {
            // return walkEq(lvalue, rvalue, false);
        } else if (op == Operator.LTEQ) {
            // return walkLtEq(lvalue, rvalue);
        } else if (op == Operator.GTEQ) {
            // return walkGtEq(lvalue, rvalue);
        } else if (op == Operator.AND) {
            if (expression instanceof MultiExpression) {
                return walkAnd(((MultiExpression) expression).values);
            } else {
                return walkAnd(lvalue, rvalue);
            }
        } else if (op == Operator.NOT) {
            // return walkNot(lvalue);
        } else if (op == Operator.OR) {
            return walkOr(lvalue, rvalue);
        } else if (op == Operator.LIKE) {
            // return walkLike(lvalue, rvalue, true, false);
        } else if (op == Operator.ILIKE) {
            // return walkLike(lvalue, rvalue, true, true);
        } else if (op == Operator.NOTLIKE) {
            // return walkLike(lvalue, rvalue, false, false);
        } else if (op == Operator.NOTILIKE) {
            // return walkLike(lvalue, rvalue, false, true);
        } else if (op == Operator.IN) {
            return walkIn(lvalue, rvalue, true);
        } else if (op == Operator.NOTIN) {
            return walkIn(lvalue, rvalue, false);
        } else if (op == Operator.ISNULL) {
            return walkNull(lvalue, true);
        } else if (op == Operator.ISNOTNULL) {
            return walkNull(lvalue, false);
        } else if (op == Operator.BETWEEN) {
            // return walkBetween(lvalue, rvalue, true);
        } else if (op == Operator.NOTBETWEEN) {
            // return walkBetween(lvalue, rvalue, false);
        }
        throw new QueryParseException("Unknown operator: " + op);
    }

    private void checkDateLiteralForCast(Operator op, Operand value, String name) {
        if (op == Operator.BETWEEN || op == Operator.NOTBETWEEN) {
            LiteralList l = (LiteralList) value;
            checkDateLiteralForCast(l.get(0), name);
            checkDateLiteralForCast(l.get(1), name);
        } else {
            checkDateLiteralForCast(value, name);
        }
    }

    private void checkDateLiteralForCast(Operand value, String name) {
        if (value instanceof DateLiteral && !((DateLiteral) value).onlyDate) {
            throw new QueryParseException("DATE() cast must be used with DATE literal, not TIMESTAMP: " + name);
        }
    }

    private QueryBuilder walkEq(Operand lvalue, Operand rvalue, boolean equals) {
        FieldInfo leftInfo = walkReference(lvalue);
        if (leftInfo.isMixinTypes()) {
            if (!(rvalue instanceof StringLiteral)) {
                throw new QueryParseException("Invalid EQ rhs: " + rvalue);
            }
            // return walkMixinTypes(Collections.singletonList((StringLiteral) rvalue), equals);
        }
        Literal convertedLiteral = convertIfBoolean(leftInfo, (Literal) rvalue);
        // If the literal is null it could be :
        // - test for non existence of boolean field
        // - a platform issue
        if (convertedLiteral == null) {
            return walkNull(lvalue, equals);
        }
        return new EqualQueryBuilder(leftInfo, convertedLiteral, equals);
        // return getQueryBuilder(leftInfo, name -> new EqualQueryBuilder(name, convertedLiteral, equals));
    }

    private QueryBuilder walkAnd(Operand lvalue, Operand rvalue) {
        return walkAnd(Arrays.asList(lvalue, rvalue));
    }

    private QueryBuilder walkAnd(List<Operand> values) {
        return new CompositionQueryBuilder(walkOperandAsExpression(values), true);
    }

    private QueryBuilder walkOr(Operand lvalue, Operand rvalue) {
        return walkOr(Arrays.asList(lvalue, rvalue));
    }

    private QueryBuilder walkOr(List<Operand> values) {
        List<QueryBuilder> children = walkOperandAsExpression(values);
        if (children.size() == 1) {
            return children.get(0);
        }
        return new CompositionQueryBuilder(children, false);
    }

    private QueryBuilder walkIn(Operand lvalue, Operand rvalue, boolean in) {
        if (!(rvalue instanceof LiteralList)) {
            throw new QueryParseException("Invalid IN, right hand side must be a list: " + rvalue);
        }
        FieldInfo leftInfo = walkReference(lvalue);
        if (leftInfo.isMixinTypes()) {
            // return walkMixinTypes((LiteralList) rvalue, in);
        }
        return new InQueryBuilder(leftInfo, (LiteralList) rvalue, in);
    }

    private QueryBuilder walkNull(Operand lvalue, boolean isNull) {
        FieldInfo leftInfo = walkReference(lvalue);
        return new IsNullQueryBuilder(leftInfo, isNull);
    }

    /**
     * Method used to walk on a list of {@link org.nuxeo.ecm.core.query.sql.model.Expression} typed as {@link Operand}.
     */
    private List<QueryBuilder> walkOperandAsExpression(List<Operand> operands) {
        return operands.stream().map(this::walkOperandAsExpression).collect(Collectors.toList());
    }

    /**
     * Method used to walk on an {@link org.nuxeo.ecm.core.query.sql.model.Expression} typed as {@link Operand}.
     */
    private QueryBuilder walkOperandAsExpression(Operand operand) {
        if (!(operand instanceof org.nuxeo.ecm.core.query.sql.model.Expression)) {
            throw new IllegalArgumentException("Operand " + operand + "is not an Expression.");
        }
        return walkExpression((org.nuxeo.ecm.core.query.sql.model.Expression) operand);
    }

    private FieldInfo walkReference(Operand value) {
        if (!(value instanceof Reference)) {
            throw new QueryParseException("Invalid query, left hand side must be a property: " + value);
        }
        return walkReference((Reference) value);
    }

    private FieldInfo walkReference(Reference reference) {
        FieldInfo fieldInfo = walkReference(reference.name);
        if (DATE_CAST.equals(reference.cast)) {
            Type type = fieldInfo.type;
            if (!(type instanceof DateType
                    || (type instanceof ListType && ((ListType) type).getFieldType() instanceof DateType))) {
                throw new QueryParseException("Cannot cast to " + reference.cast + ": " + reference.name);
            }
        }
        return fieldInfo;
    }

    private FieldInfo walkReference(String name) {
        String prop = canonicalXPath(name);
        String[] parts = prop.split("/");
        if (prop.startsWith(NXQL.ECM_PREFIX)) {
            if (prop.startsWith(NXQL.ECM_ACL + "/")) {
                // return parseACP(prop, parts);
            }
            String field = DBSSession.convToInternal(prop);
            return new FieldInfo(prop, field);
        }

        // Copied from Mongo

        String first = parts[0];
        Field field = schemaManager.getField(first);
        if (field == null) {
            if (first.indexOf(':') > -1) {
                throw new QueryParseException("No such property: " + name);
            }
            // check without prefix
            // TODO precompute this in SchemaManagerImpl
            for (Schema schema : schemaManager.getSchemas()) {
                if (schema == null || !StringUtils.isBlank(schema.getNamespace().prefix)) {
                    // schema with prefix, do not consider as candidate
                    continue;
                }
                field = schema.getField(first);
                if (field != null) {
                    break;
                }
            }
            if (field == null) {
                throw new QueryParseException("No such property: " + name);
            }
        }
        Type type = field.getType();
        // canonical name
        parts[0] = field.getName().getPrefixedName();
        // are there wildcards or list indexes?
        boolean firstPart = true;
        for (String part : parts) {
            if (NumberUtils.isDigits(part)) {
                // explicit list index
                type = ((ListType) type).getFieldType();
            } else if (!part.startsWith("*")) {
                // complex sub-property
                if (!firstPart) {
                    // we already computed the type of the first part
                    field = ((ComplexType) type).getField(part);
                    if (field == null) {
                        throw new QueryParseException("No such property: " + name);
                    }
                    type = field.getType();
                }
            } else {
                // wildcard
                type = ((ListType) type).getFieldType();
            }
            firstPart = false;
        }
        String fullField = String.join("/", parts);
        return new FieldInfo(prop, fullField, type, false);
    }

    /**
     * Canonicalizes a Nuxeo-xpath. Replaces {@code a/foo[123]/b} with {@code a/123/b} A star or a star followed by
     * digits can be used instead of just the digits as well.
     *
     * @param xpath the xpath
     * @return the canonicalized xpath.
     */
    private String canonicalXPath(String xpath) {
        while (xpath.length() > 0 && xpath.charAt(0) == '/') {
            xpath = xpath.substring(1);
        }
        if (xpath.indexOf('[') == -1) {
            return xpath;
        } else {
            return NON_CANON_INDEX.matcher(xpath).replaceAll("$1");
        }
    }

    private Literal convertIfBoolean(FieldInfo fieldInfo, Literal literal) {
        if (fieldInfo.type instanceof BooleanType && literal instanceof IntegerLiteral) {
            long value = ((IntegerLiteral) literal).value;
            if (ZERO.equals(value)) {
                literal = fieldInfo.isTrueOrNullBoolean ? null : new BooleanLiteral(false);
            } else if (ONE.equals(value)) {
                literal = new BooleanLiteral(true);
            } else {
                throw new QueryParseException("Invalid boolean: " + value);
            }
        }
        return literal;
    }

    // private QueryBuilder getQueryBuilder(FieldInfo fieldInfo, Function<String, QueryBuilder> constraintBuilder) {
    // Matcher m = WILDCARD_SPLIT.matcher(fieldInfo.fullField);
    // if (m.matches()) {
    // String correlatedFieldPart = m.group(1);
    // String fieldSuffix = m.group(2);
    // if (fieldSuffix == null) {
    // fieldSuffix = fieldInfo.queryField.substring(fieldInfo.queryField.lastIndexOf('/') + 1);
    // }
    // String path = fieldInfo.queryField.substring(0, fieldInfo.queryField.indexOf('/' + fieldSuffix));
    // return new CorrelatedContainerQueryBuilder(path, correlatedFieldPart, constraintBuilder.apply(fieldSuffix));
    // }
    // String path = fieldInfo.queryField;
    // Handle the list type case - if it's not present in path
    // if (fieldInfo.type != null && fieldInfo.type.isListType() && !fieldInfo.fullField.endsWith("*")) {
    // path += '/' + MarkLogicHelper.buildItemNameFromPath(path);
    // }
    // return constraintBuilder.apply(fieldInfo.fullField);
    // }

    private static class FieldInfo {

        /** NXQL property. */
        private final String prop;

        /** Internal field including wildcards. */
        protected final String fullField;

        /** Prefix before the wildcard, may be null. */
        protected final String fieldPrefix;

        /** Wildcard part after, may be null. * */
        protected final String fieldWildcard;

        /**
         * Field to query:
         * <ul>
         * <li>foo/0/bar -> foo.0.bar</li>
         * <li>foo / * / bar -> foo.bar</li>
         * <li>foo / *1 / bar -> bar</li>
         * </ul>
         */
        protected final String queryField;

        protected final Type type;

        /** Boolean system properties only use TRUE or NULL, not FALSE, so queries must be updated accordingly. */
        protected final boolean isTrueOrNullBoolean;

        /**
         * Constructor for a simple field.
         */
        public FieldInfo(String prop, String field) {
            this(prop, field, DBSSession.getType(field), true);
        }

        public FieldInfo(String prop, String fullField, Type type, boolean isTrueOrNullBoolean) {
            this.prop = prop;
            this.fullField = fullField;
            this.type = type;
            this.isTrueOrNullBoolean = isTrueOrNullBoolean;
            Matcher m = WILDCARD_SPLIT.matcher(fullField);
            if (m.matches()) {
                fieldPrefix = m.group(1);
                fieldWildcard = m.group(2);
                queryField = m.group(3);
            } else {
                fieldPrefix = null;
                fieldWildcard = null;
                queryField = fullField;
            }
        }

        public boolean isBoolean() {
            return type instanceof BooleanType;
        }

        public boolean isMixinTypes() {
            return fullField.equals(KEY_MIXIN_TYPES);
        }

        public boolean hasWildcard() {
            return fieldPrefix != null;
        }

    }

    private static class EqualQueryBuilder implements QueryBuilder {

        private FieldInfo fieldInfo;

        private Literal literal;

        private boolean equals;

        public EqualQueryBuilder(FieldInfo fieldInfo, Literal literal, boolean equals) {
            this.fieldInfo = fieldInfo;
            this.literal = literal;
            this.equals = equals;
        }

        @Override
        public Expression build() {
            Expression expression = Expression.i(fieldInfo.queryField);
            if (literal instanceof BooleanLiteral) {
                expression = expression.eq(((BooleanLiteral) literal).value);
            } else if (literal instanceof DateLiteral) {
                // expression = expression.eq(((DateLiteral) literal).value);
            } else if (literal instanceof DoubleLiteral) {
                expression = expression.eq(((DoubleLiteral) literal).value);
            } else if (literal instanceof IntegerLiteral) {
                expression = expression.eq(((IntegerLiteral) literal).value);
            } else if (literal instanceof StringLiteral) {
                expression = expression.eq(((StringLiteral) literal).value);
            } else {
                throw new QueryParseException("Unknown literal: " + literal);
            }
            return expression;
        }

        @Override
        public void not() {
            equals = !equals;
        }

    }

    private static class InQueryBuilder implements QueryBuilder {

        private FieldInfo fieldInfo;

        private LiteralList literals;

        private boolean in;

        public InQueryBuilder(FieldInfo fieldInfo, LiteralList literals, boolean in) {
            this.fieldInfo = fieldInfo;
            this.literals = literals;
            this.in = in;
        }

        @Override
        public Expression build() {
            Expression expression = Expression.i(fieldInfo.queryField);
            String[] values = literals.stream().map(this::getLiteralValue).map(Object::toString).toArray(String[]::new);
            return expression.in(Expression.par(Expression.s(values)));
        }

        @Override
        public void not() {
            in = !in;
        }

    }

    private class IsNullQueryBuilder implements QueryBuilder {

        private FieldInfo fieldInfo;

        private boolean isNull;

        public IsNullQueryBuilder(FieldInfo fieldInfo, boolean isNull) {
            this.fieldInfo = fieldInfo;
            this.isNull = isNull;
        }

        @Override
        public Expression build() {
            Expression identifier = Expression.i(fieldInfo.queryField);
            if (isNull) {
                return identifier.isNull();
            }
            return identifier.isNotNull();
        }

        @Override
        public void not() {
            isNull = !isNull;
        }

    }

    private static class CompositionQueryBuilder implements QueryBuilder {

        private boolean and;

        private List<QueryBuilder> queryBuilders;

        public CompositionQueryBuilder(List<QueryBuilder> queryBuilders, boolean and) {
            this.queryBuilders = queryBuilders;
            this.and = and;
        }

        @Override
        public Expression build() {
            BinaryOperator<Expression> reduce = Expression::or;
            if (and) {
                reduce = Expression::and;
            }
            return queryBuilders.stream().map(QueryBuilder::build).reduce(reduce).get();
        }

        @Override
        public void not() {
            and = !and;
            queryBuilders.forEach(QueryBuilder::not);
        }
    }

    private interface QueryBuilder {

        Expression build();

        void not();

        default Object getLiteralValue(Literal literal) {
            Object result;
            if (literal instanceof BooleanLiteral) {
                result = ((BooleanLiteral) literal).value;
            } else if (literal instanceof DateLiteral) {
                result = ((DateLiteral) literal).value;
            } else if (literal instanceof DoubleLiteral) {
                result = ((DoubleLiteral) literal).value;
            } else if (literal instanceof IntegerLiteral) {
                result = ((IntegerLiteral) literal).value;
            } else if (literal instanceof StringLiteral) {
                result = ((StringLiteral) literal).value;
            } else {
                throw new QueryParseException("Unknown literal: " + literal);
            }
            return result;
        }

    }

}
