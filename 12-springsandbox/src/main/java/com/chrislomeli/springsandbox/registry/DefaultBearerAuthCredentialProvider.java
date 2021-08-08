package com.chrislomeli.springsandbox.registry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.SaslConfigs;

import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class DefaultBearerAuthCredentialProvider implements BearerAuthCredentialProvider {

    final static public String TOKEN_PROVIDER_NAME = "NIKE_BEARER_TOKEN";
    final static ObjectMapper objectMapper = new ObjectMapper();

    String url;
    String requestBody;
    AuthorizationTokenCache.AuthorizationToken authorizationToken;

    public String getAuthorizationToken()  {

         if (authorizationToken != null && authorizationToken.isValid()) {
            return authorizationToken.getAccess_token();
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .version(HttpClient.Version.HTTP_1_1)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        Optional<String> token = sendRequest(request);
        // todo: decide a better way to handle no token due to error
        return token.orElse(null);
    }

    /*
      The idea here is that if we don't have an existing token - then wait for the token.
      but if we have a pre-existing token, just request a new one on a Future<>, but use the existing one this time
      todo: I have not tested that the future will update the AuthorizationToken once it completes, but it should work
     */
    private Optional<String> sendRequest(HttpRequest request) {
        try {
            if (authorizationToken == null || authorizationToken.getAccess_token().isEmpty()) {
                HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
                return storeAuthorization(response);
            } else {
                // let the future store a new access token
                CompletableFuture<Void> response = HttpClient.newHttpClient()
                        .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                        .thenAccept(this::storeAuthorization);
                // in the meantime, use the one we have now
                return Optional.of(authorizationToken.getAccess_token());
            }
        } catch (Exception e) {
            log.error("Error trying to call token server", e);
            return Optional.empty();
        }
    }

    private Optional<String> storeAuthorization(HttpResponse<String> response) {
        if (response.statusCode() >= 300) {
            log.error("Received an error {} from the token server {}",response.statusCode(), response.body());
            return Optional.empty();
        }
        try {
           AuthorizationTokenCache.auth = objectMapper.readValue(response.body(), AuthorizationTokenCache.AuthorizationToken.class);
           return Optional.of(AuthorizationTokenCache.auth.getAccess_token());
        } catch (JsonProcessingException e) {
            log.error("Cannot get a token from Nike server",e);
            return Optional.empty();
        }
    }

    @Override
    public String alias() {
        return TOKEN_PROVIDER_NAME;
    }

    @Override
    public String getBearerToken(URL url) {
        return getAuthorizationToken();
    }

    @Override
    public void configure(Map<String, ?> map) {
        authorizationToken = AuthorizationTokenCache.auth;
        String clientId="";
        String clientSecret="";

        String[] jassStrings = ((String) map.get(SaslConfigs.SASL_JAAS_CONFIG)).split(" ");
        for (String s : jassStrings) {
            if (s.isEmpty() || !s.contains("="))
                continue;
            var value = s.substring(s.indexOf("=")+1).replaceAll("\"", "");
            if (s.startsWith("oauth.token"))
                url = value;
            else if (s.startsWith("oauth.client.id"))
                clientId = value;
            else if (s.startsWith("oauth.client.secret"))
                clientSecret = value;
        }
        requestBody = String.format("grant_type=client_credentials&client_id=%s&client_secret=%s", clientId, clientSecret);
    }
}

