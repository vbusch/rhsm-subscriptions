/*
 * Copyright Red Hat, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 * Red Hat trademarks are not licensed under GPLv3. No permission is
 * granted to use or replicate Red Hat trademarks that are incorporated
 * in this software or its documentation.
 */
package org.candlepin.subscriptions.actuator;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.candlepin.subscriptions.util.TlsProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.autoconfigure.info.ConditionalOnEnabledInfoContributor;
import org.springframework.boot.actuate.autoconfigure.info.InfoContributorFallback;
import org.springframework.boot.actuate.info.Info.Builder;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

/** Endpoint to print basic information about the certificates Conduit is using. */
@Component
@ConditionalOnEnabledInfoContributor(value = "certs", fallback = InfoContributorFallback.DISABLE)
public class CertInfoContributor implements InfoContributor {
  private static final Logger log = LoggerFactory.getLogger(CertInfoContributor.class);
  public static final String CERT_LOAD_ERR = "Could not load certificates";

  private final ApplicationContext context;

  public CertInfoContributor(ApplicationContext context) {
    this.context = context;
  }

  public Map<String, Map<String, String>> storeInfo(Resource store, char[] password)
      throws IllegalStateException {
    try {
      if (Objects.nonNull(store)) {
        String filename = store.getFilename();
        if (Objects.nonNull(filename) && !filename.isBlank()) {
          /* The ${clowder.endpoints...trust-store-path property is actually a synthetic property created by our
           * ClowderJsonPropertySource for the endpoints.  The value is based off of a Java truststore we create from the
           * path given in tlsCACert and the presence of a tlsPort key in the endpoint section.  Since tlsCACert or tlsPort
           * isn't always present, the trust-store-path might not resolve to an actual path name. */
          if ((filename.startsWith("${clowder.endpoints")
                  || filename.startsWith("${clowder.privateEndpoints"))
              && filename.endsWith("trust-store-path}")) {
            return Map.of("Unresolved clowder config value: " + filename, Collections.emptyMap());
          }
          if (!store.isReadable()) {
            return Map.of(filename + " not readable", Collections.emptyMap());
          }
          return CertInfoInquisitor.loadStoreInfo(store, password);
        }
      }
      return Collections.emptyMap();
    } catch (IOException | GeneralSecurityException e) {
      throw new IllegalStateException(CERT_LOAD_ERR, e);
    }
  }

  @Override
  public void contribute(Builder builder) {
    var clientPropertiesBeans = context.getBeansOfType(TlsProperties.class);
    for (Entry<String, TlsProperties> entry : clientPropertiesBeans.entrySet()) {
      String name = entry.getKey();
      TlsProperties config = entry.getValue();
      try {
        builder.withDetail(
            name + ".keystore", storeInfo(config.getKeystore(), config.getKeystorePassword()));
      } catch (IllegalStateException e) {
        log.info(CERT_LOAD_ERR, e);
        builder.withDetail(name + ".keystore", e.getMessage() + ":" + e.getCause().getMessage());
      }

      try {
        builder.withDetail(
            name + ".truststore",
            storeInfo(config.getTruststore(), config.getTruststorePassword()));
      } catch (IllegalStateException e) {
        log.info(CERT_LOAD_ERR, e);
        builder.withDetail(name + ".truststore", e.getMessage() + ":" + e.getCause().getMessage());
      }
    }
  }
}
