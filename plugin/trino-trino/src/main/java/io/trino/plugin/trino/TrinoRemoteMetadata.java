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

import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.ConnectorExpression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.trino.TrinoRemoteSessionProperties.isProjectionPushdownEnabled;

public class TrinoRemoteMetadata
        extends DefaultJdbcMetadata
{
    private final JdbcClient jdbcClient;
    private final boolean precalculateStatisticsForPushdown;

    public TrinoRemoteMetadata(JdbcClient jdbcClient, TimestampTimeZoneDomain timestampTimeZoneDomain, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        this(jdbcClient, timestampTimeZoneDomain, false, jdbcQueryEventListeners);
    }

    public TrinoRemoteMetadata(
            JdbcClient jdbcClient,
            TimestampTimeZoneDomain timestampTimeZoneDomain,
            boolean precalculateStatisticsForPushdown,
            Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(jdbcClient, timestampTimeZoneDomain, precalculateStatisticsForPushdown, jdbcQueryEventListeners);
        this.jdbcClient = jdbcClient;
        this.precalculateStatisticsForPushdown = precalculateStatisticsForPushdown;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof JdbcProcedureHandle procedureHandle) {
            return procedureHandle.getColumns().orElseThrow().stream()
                    .collect(toImmutableMap(columnHandle -> columnHandle.getColumnMetadata().getName(), identity()));
        }

        return jdbcClient.getColumns(session, (JdbcTableHandle) tableHandle).stream()
                .collect(toImmutableMap(columnHandle -> columnHandle.getColumnMetadata().getName(), identity()));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        if (!isProjectionPushdownEnabled(session)) {
            return Optional.empty();
        }
        return super.applyProjection(session, table, projections, assignments);
    }
}
