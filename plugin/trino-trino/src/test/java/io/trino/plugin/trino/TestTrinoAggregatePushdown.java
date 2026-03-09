/*
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
 */
package io.trino.plugin.trino;

import io.trino.FeaturesConfig;
import io.trino.metadata.TypeRegistry;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.type.InternalTypeManager;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTrinoAggregatePushdown
{
    private static final TypeOperators typeOperators = new TypeOperators();
    private static final TypeRegistry typeRegistry = new TypeRegistry(typeOperators, new FeaturesConfig());
    private static final TypeManager TYPE_MANAGER = new InternalTypeManager(typeRegistry);

    private static final JdbcColumnHandle BIGINT_COLUMN =
            JdbcColumnHandle.builder()
                    .setColumnName("columnName_bigInt")
                    .setColumnType(BIGINT)
                    .setJdbcTypeHandle(new JdbcTypeHandle(Types.BIGINT, Optional.of("int8"), Optional.of(0), Optional.of(0), Optional.empty(), Optional.empty()))
                    .build();

    private static final JdbcColumnHandle DOUBLE_COLUMN =
            JdbcColumnHandle.builder()
                    .setColumnName("columnName_double")
                    .setColumnType(DOUBLE)
                    .setJdbcTypeHandle(new JdbcTypeHandle(Types.DOUBLE, Optional.of("double"), Optional.of(0), Optional.of(0), Optional.empty(), Optional.empty()))
                    .build();

    private static JdbcClient jdbcClient;

    static {
        try {
            jdbcClient = new TrinoRemoteClient(
                    new BaseJdbcConfig(),
                    new TrinoRemoteConfig(),
                    identity -> { throw new UnsupportedOperationException(); },
                    new DefaultQueryBuilder(RemoteQueryModifier.NONE),
                    TYPE_MANAGER,
                    new DefaultIdentifierMapping(),
                    RemoteQueryModifier.NONE);
        }
        catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    public void testImplementCount()
    {
        Variable bigintVariable = new Variable("v_bigint", BIGINT);
        Variable doubleVariable = new Variable("v_double", BIGINT);
        Optional<ConnectorExpression> filter = Optional.of(new Variable("a_filter", BOOLEAN));

        // count(*)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty()),
                Map.of(),
                Optional.of("count(*)"));

        // count(bigint)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(bigintVariable), List.of(), false, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.of("count(\"columnName_bigInt\")"));

        // count(double)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("count(\"columnName_double\")"));

        // count(DISTINCT bigint) --> distinct not supported
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(bigintVariable), List.of(), true, filter),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.empty());

        // count() FILTER (WHERE ...) --> filters not supported
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(), List.of(), false, filter),
                Map.of(),
                Optional.empty());

        // count(bigint) FILTER (WHERE ...) --> filters not supported
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(bigintVariable), List.of(), false, filter),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.empty());
    }

    @Test
    public void testImplementSum()
    {
        Variable bigintVariable = new Variable("v_bigint", BIGINT);
        Variable doubleVariable = new Variable("v_double", DOUBLE);
        Optional<ConnectorExpression> filter = Optional.of(new Variable("a_filter", BOOLEAN));

        // sum(bigint)
        testImplementAggregation(
                new AggregateFunction("sum", BIGINT, List.of(bigintVariable), List.of(), false, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.of("sum(\"columnName_bigInt\")"));

        // sum(double)
        testImplementAggregation(
                new AggregateFunction("sum", DOUBLE, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("sum(\"columnName_double\")"));

        // sum(DISTINCT bigint) --> distinct not supported
        testImplementAggregation(
                new AggregateFunction("sum", BIGINT, List.of(bigintVariable), List.of(), true, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.of("sum(DISTINCT \"columnName_bigInt\")"));

        // sum(bigint) FILTER (WHERE ...) --> filters not supported
        testImplementAggregation(
                new AggregateFunction("sum", BIGINT, List.of(bigintVariable), List.of(), false, filter),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.empty());
    }

    @Test
    public void testImplementAvg()
    {
        Variable bigintVariable = new Variable("v_bigint", BIGINT);
        Variable doubleVariable = new Variable("v_double", DOUBLE);
        Optional<ConnectorExpression> filter = Optional.of(new Variable("a_filter", BOOLEAN));

        // avg(double)
        testImplementAggregation(
                new AggregateFunction("avg", DOUBLE, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("avg(\"columnName_double\")"));

        // avg(bigint)
        testImplementAggregation(
                new AggregateFunction("avg", DOUBLE, List.of(bigintVariable), List.of(), false, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.of("avg(CAST(\"columnName_bigInt\" AS double precision))"));

        // avg(DISTINCT double) --> distinct not supported
        testImplementAggregation(
                new AggregateFunction("avg", DOUBLE, List.of(doubleVariable), List.of(), true, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.empty());

        // sum(double) FILTER (WHERE ...) --> filters not supported
        testImplementAggregation(
                new AggregateFunction("avg", DOUBLE, List.of(doubleVariable), List.of(), false, filter),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.empty());
    }

    @Test
    public void testImplementMinMax()
    {
        Variable bigintVariable = new Variable("v_bigint", BIGINT);
        Variable doubleVariable = new Variable("v_double", DOUBLE);
        Optional<ConnectorExpression> filter = Optional.of(new Variable("a_filter", BOOLEAN));

        // min(double)
        testImplementAggregation(
                new AggregateFunction("min", DOUBLE, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("min(\"columnName_double\")"));

        // max(double)
        testImplementAggregation(
                new AggregateFunction("max", DOUBLE, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("max(\"columnName_double\")"));

        // min(bigint)
        testImplementAggregation(
                new AggregateFunction("min", BIGINT, List.of(bigintVariable), List.of(), false, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.of("min(\"columnName_bigInt\")"));

        // max(bigint)
        testImplementAggregation(
                new AggregateFunction("max", BIGINT, List.of(bigintVariable), List.of(), false, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.of("max(\"columnName_bigInt\")"));

        // min( DISTINCT double) --> distinct not supported
        testImplementAggregation(
                new AggregateFunction("min", DOUBLE, List.of(doubleVariable), List.of(), true, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.empty());

        // max(double) FILTER (WHERE ...) --> filters not supported
        testImplementAggregation(
                new AggregateFunction("max", DOUBLE, List.of(doubleVariable), List.of(), false, filter),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.empty());
    }

    private void testImplementAggregation(AggregateFunction aggregateFunction, Map<String, ColumnHandle> assignments, Optional<String> expectedExpression)
    {
        Optional<JdbcExpression> result = jdbcClient.implementAggregation(SESSION, aggregateFunction, assignments);
        if (expectedExpression.isEmpty()) {
            assertTrue(result.isEmpty());
        }
        else {
            assertTrue(result.isPresent());
            assertEquals(result.get().getExpression(), expectedExpression.get());
            Optional<ColumnMapping> columnMapping = jdbcClient.toColumnMapping(SESSION, null, result.get().getJdbcTypeHandle());
            assertTrue(columnMapping.isPresent(), "No mapping for: " + result.get().getJdbcTypeHandle());
            assertEquals(columnMapping.get().getType(), aggregateFunction.getOutputType());
        }
    }
}
