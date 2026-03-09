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

import com.google.inject.Inject;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;

public class TrinoDriverConnectionFactory
        extends DriverConnectionFactory
{
    @Inject
    public TrinoDriverConnectionFactory(Driver driver, String connectionUrl, Properties connectionProperties, CredentialPropertiesProvider credentialPropertiesProvider, OpenTelemetry openTelemetry)
    {
        super(driver, connectionUrl, connectionProperties, credentialPropertiesProvider, openTelemetry);
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Connection connection = super.openConnection(session);
        checkState(connection != null, "Driver returned null connection");
        if (connection != null) {
            String currentClientTags = nullToEmpty(connection.getClientInfo("ClientTags"));
            connection.setClientInfo("ClientTags", currentClientTags + ",parent_query_id:" + session.getQueryId());
            //connection.setNetworkTimeout(directExecutor(), (int) session.getProperty(NETWORK_TIMEOUT, Duration.class).toMillis());
        }
        return connection;
    }
}
