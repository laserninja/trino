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
package io.trino.plugin.trino.authentication;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class TrinoRemoteImpersonateOAuth2CredentialPropertiesProvider
        implements CredentialPropertiesProvider
{
    private final CredentialProvider credentialProvider;

    public TrinoRemoteImpersonateOAuth2CredentialPropertiesProvider(CredentialProvider credentialProvider)
    {
        this.credentialProvider = credentialProvider;
    }

    public Map<String, String> getCredentialProperties(ConnectorIdentity identity)
    {
        Builder<String, String> properties = ImmutableMap.<String, String>builder();
        credentialProvider.getConnectionUser(Optional.of(identity)).ifPresent((user) -> {
            properties.put("user", user);
        });

        String token = identity.getExtraCredentials().getOrDefault("trino-remote.oauth2.access_token.passthrough", "");
        if (token.isEmpty()) {
            throw new TrinoException(GENERIC_USER_ERROR, "Token pass-through authentication requires a valid token");
        }
        properties.put("accessToken", token);
        properties.put("sessionUser", identity.getUser());
        return properties.buildOrThrow();
    }
}
