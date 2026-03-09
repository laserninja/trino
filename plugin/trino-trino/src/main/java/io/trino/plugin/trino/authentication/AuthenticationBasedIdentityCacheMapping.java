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

import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.spi.connector.ConnectorSession;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

public class AuthenticationBasedIdentityCacheMapping
        implements IdentityCacheMapping
{
    public IdentityCacheKey getRemoteUserCacheKey(ConnectorSession session)
    {
        return new Key(session.getIdentity().getUser(),
                Optional.ofNullable(session.getIdentity().getPrincipal().map(principal -> principal.getName()).orElse(null)));
    }

    private static class Key
            extends IdentityCacheKey
    {
        private final String user;
        private final Optional<String> principalName;

        public Key(String user, Optional<String> principalName)
        {
            this.user = requireNonNull(user, "user is null");
            this.principalName = (Optional) requireNonNull(principalName, "principalName is null");
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) {
                return true;
            }
            else if (object != null && this.getClass() == object.getClass()) {
                Key key = (Key) object;
                return Objects.equals(user, key.user) && Objects.equals(principalName, key.principalName);
            }
            else {
                return false;
            }
        }

        public int hashCode()
        {
            return hash(user, principalName);
        }
    }
}
