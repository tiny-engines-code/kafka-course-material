package com.chrislomeli.springsandbox.registry;

import lombok.Data;

import java.time.Instant;

public class AuthorizationTokenCache {

    @Data
    static class  AuthorizationToken {
        String token_type;
        String access_token;
        int expires_in;
        String scope;
        Instant timeStamp;

        public AuthorizationToken() {
            this.timeStamp = Instant.now();
        }

        public boolean isValid() {
            return !access_token.isEmpty() && Instant.now().isBefore(timeStamp.plusSeconds(expires_in - 60));
        }
    }

    static AuthorizationToken auth;

}
