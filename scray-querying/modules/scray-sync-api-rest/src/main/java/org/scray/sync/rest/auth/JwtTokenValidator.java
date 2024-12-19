// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.scray.sync.rest.auth;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Base64;

import static java.util.Objects.isNull;


@RequiredArgsConstructor
public class JwtTokenValidator
{
    private static final Logger logger = LoggerFactory.getLogger(JwtTokenValidator.class);

    private final JwkProvider jwkProvider;

    public AccessToken validateAuthorizationHeader(String authorizationHeader)
        throws InvalidTokenException
    {
        String tokenValue = subStringBearer(authorizationHeader);
        validateToken(tokenValue);
        return new AccessToken(tokenValue);
    }


    private String subStringBearer(String authorizationHeader)
    {
        try
        {
            return authorizationHeader.substring(AccessToken.BEARER.length());
        }
        catch (Exception ex)
        {
            throw new InvalidTokenException("There is no AccessToken in a request header");
        }
    }


    private void validateToken(String value)
    {
        DecodedJWT decodedJWT = decodeToken(value);
        verifyTokenHeader(decodedJWT);
        verifySignature(decodedJWT);
        verifyPayload(decodedJWT);
    }


    private DecodedJWT decodeToken(String value)
    {
        if (isNull(value))
        {
            throw new InvalidTokenException("Token has not been provided");
        }

        DecodedJWT decodedJWT = JWT.decode(value);
        logger.debug("Token decoded successfully");
        return decodedJWT;
    }


    private void verifyTokenHeader(DecodedJWT decodedJWT)
    {
        try
        {
            Preconditions.checkArgument(decodedJWT.getType().equals("JWT"));
            logger.debug("Token's header is correct");
        }
        catch (IllegalArgumentException ex)
        {
            throw new InvalidTokenException("Token is not JWT type", ex);
        }
    }


    private void verifySignature(DecodedJWT decodedJWT)
    {
        try
        {
            Jwk jwk = jwkProvider.get(decodedJWT.getKeyId());
            Algorithm algorithm = Algorithm.RSA256((RSAPublicKey)jwk.getPublicKey(), null);
            algorithm.verify(decodedJWT);
            logger.debug("Token's signature is correct");
        }
        catch (JwkException | SignatureVerificationException ex)
        {
            throw new InvalidTokenException("Token has invalid signature", ex);
        }
    }


    private void verifyPayload(DecodedJWT decodedJWT)
    {

        JsonObject payloadAsJson = decodeTokenPayloadToJsonObject(decodedJWT);

        if (hasTokenExpired(payloadAsJson))
        {
            throw new InvalidTokenException("Token has expired");
        }
        logger.debug("Token has not expired");

        if (!hasTokenRealmRolesClaim(payloadAsJson))
        {
            throw new InvalidTokenException("Token doesn't contain claims with realm roles");
        }
        logger.debug("Token's payload contain claims with realm roles");

        if (!hasTokenScopeInfo(payloadAsJson))
        {
            throw new InvalidTokenException("Token doesn't contain scope information");
        }
        logger.debug("Token's payload contain scope information");
    }


    private JsonObject decodeTokenPayloadToJsonObject(DecodedJWT decodedJWT)
    {
        try
        {
            String payloadAsString = decodedJWT.getPayload();
            return new Gson().fromJson(
                                       new String(Base64.getDecoder().decode(payloadAsString), StandardCharsets.UTF_8),
                                       JsonObject.class);
        }
        catch (RuntimeException exception)
        {
            throw new InvalidTokenException("Invalid JWT or JSON format of each of the jwt parts", exception);
        }
    }


    private boolean hasTokenExpired(JsonObject payloadAsJson)
    {

        Instant expirationDatetime = extractExpirationDate(payloadAsJson);
        return Instant.now().isAfter(expirationDatetime);
    }


    private Instant extractExpirationDate(JsonObject payloadAsJson)
    {
        try
        {
            return Instant.ofEpochSecond(payloadAsJson.get("exp").getAsLong());
        }
        catch (NullPointerException ex)
        {
            throw new InvalidTokenException("There is no 'exp' claim in the token payload");
        }
    }


    private boolean hasTokenRealmRolesClaim(JsonObject payloadAsJson)
    {
        try
        {
            return payloadAsJson.getAsJsonObject("realm_access").getAsJsonArray("roles").size() > 0;
        }
        catch (NullPointerException ex)
        {
            return false;
        }
    }


    private boolean hasTokenScopeInfo(JsonObject payloadAsJson)
    {
        return payloadAsJson.has("scope");
    }
}
