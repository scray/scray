package org.scray.integration.ai.agent.clients.rest;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import javax.net.ssl.*;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Map;

import org.bouncycastle.mime.BoundaryLimitedInputStream;
import org.scray.integration.ai.agent.AiIntegrationAgent;
import org.apache.commons.io.input.BoundedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;


public class RestClient
{

    private final Logger logger = LoggerFactory.getLogger(AiIntegrationAgent.class);
    private static final int MAX_RESPONSE_BYTES = 1 * 1024 * 1024 * 1024; // 1 GB, tune to your data

    private URI url = null;

    private String TOKEN = "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkphbmUgRG9lIiwiaWF0IjoxNzI5NDQwMDAwLCJleHAiOjE3Mjk0NDM2MDAsImlzcyI6ImF1dGguZXhhbXBsZS5jb20iLCJhdWQiOiJhcGkuZXhhbXBsZS5jb20iLCJzY29wZSI6InJlYWQ6dGhpbmdzIHdyaXRlOnRoaW5ncyJ9";
    private static final ObjectMapper jsonObjectMapper = new ObjectMapper();

    private static void disableCertValidation()
        throws Exception
    {
        TrustManager[] trustAll = new TrustManager[] {
                                                       new X509TrustManager()
                                                       {
                                                           public X509Certificate[] getAcceptedIssuers()
                                                           {
                                                               return new X509Certificate[0];
                                                           }


                                                           public void checkClientTrusted(X509Certificate[] certs, String authType)
                                                           {
                                                           }


                                                           public void checkServerTrusted(X509Certificate[] certs, String authType)
                                                           {
                                                           }
                                                       }
        };
        SSLContext sc = SSLContext.getInstance("TLS");
        sc.init(null, trustAll, new SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

        // disable hostname verification
        HostnameVerifier allHostsValid = (hostname, session) -> true;
        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
    }


    public RestClient()
    {

        String scraySyncApiUrl = System.getenv("SCRAY_SYNC_API_URL");

        if (scraySyncApiUrl != null && !scraySyncApiUrl.isEmpty())
        {

            try
            {
                this.url = new URI(scraySyncApiUrl);
            }
            catch (URISyntaxException e)
            {
                logger.warn("SCRAY_SYNC_API_URL is not a valid URL {}", url);
            }

        }
        else
        {
            try
            {
                this.url = new URI("http://localhost:8082");
            }
            catch (URISyntaxException e)
            {
                e.printStackTrace();
            }
        }
    }


    public BoundedInputStream getAllLatestData()
        throws IOException
    {
        String output = null;

        System.out.println(url);

        try
        {
            disableCertValidation(); // Hack
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } // only for testing!
        HttpURLConnection conn = (HttpURLConnection)url.resolve("sync/versioneddata/all/latest").toURL()
                                                       .openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Authorization", "Bearer " + TOKEN);

        if (conn.getResponseCode() != 200)
        {
            throw new RuntimeException("Failed : HTTP Error code : " + conn.getResponseCode());
        }

        // Check declard content length
        int declared = conn.getContentLength();
        if (declared > MAX_RESPONSE_BYTES)
        {
            conn.disconnect();
            throw new IOException("Declared Content-Length too large: " + declared);
        }

        InputStream raw = conn.getInputStream();
        BoundedInputStream inputData = BoundedInputStream.builder()
                                                  .setInputStream(raw)
                                                  .setMaxCount(MAX_RESPONSE_BYTES)
                                                  .setPropagateClose(true)
                                                  .get();
        return inputData;
    }

    /**
     * Return the searched data as an InputStream (Copy of the api data)
     * @param env
     * @param state
     * @return
     * @throws IOException
     */
    public InputStream fetchLatestVersionedDataByState(String env, String state) throws IOException {
        String payload = jsonObjectMapper.writeValueAsString(
                Map.of("filter", "data.processingEnv==" + env + ";data.state==" + state)
        );

        URL endpoint = url.resolve("/indexes/state-env/search/").toURL();
        logger.debug("Request {}", endpoint);

        try {
            disableCertValidation();
        } catch (Exception e) {
            logger.warn("Failed to disable cert validation", e);
        }

        HttpURLConnection conn = (HttpURLConnection) endpoint.openConnection();
        try {
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Authorization", "Bearer " + TOKEN);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(10000);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.getBytes(StandardCharsets.UTF_8));
            }

            int code = conn.getResponseCode();
            if (code != 200) {
                // drain error stream to free the connection
                try (InputStream err = conn.getErrorStream()) {
                    if (err != null) err.transferTo(OutputStream.nullOutputStream());
                }
                throw new IOException("Failed: HTTP error code " + code);
            }

            int declaredLength = conn.getContentLength();
            if (declaredLength > MAX_RESPONSE_BYTES) {
                throw new IOException("Declared Content-Length too large: " + declaredLength);
            }

            // Read at most MAX_RESPONSE_BYTES + 1 so we can detect overflow
            try (InputStream raw = conn.getInputStream();
                 BoundedInputStream bounded = BoundedInputStream.builder()
                         .setInputStream(raw)
                         .setMaxCount(MAX_RESPONSE_BYTES + 1L)
                         .setPropagateClose(true)
                         .get();
                 ByteArrayOutputStream buffer = new ByteArrayOutputStream(
                         declaredLength > 0 ? declaredLength : 8192)) {

                bounded.transferTo(buffer);

                if (buffer.size() > MAX_RESPONSE_BYTES) {
                    throw new IOException("Response exceeded max size of " + MAX_RESPONSE_BYTES + " bytes");
                }
                return new ByteArrayInputStream(buffer.toByteArray());
            }
        } finally {
            conn.disconnect();
        }
    }



    public void putData(String data)
        throws IOException
    {

        try
        {
            disableCertValidation(); // Hack
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        HttpURLConnection conn = (HttpURLConnection)url.resolve("/sync/versioneddata/latest").toURL().openConnection();
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Authorization", "Bearer " + TOKEN);
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream())
        {
            byte[] input = data.getBytes("utf-8");
            os.write(input, 0, input.length);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        if (conn.getResponseCode() != 200)
        {
            throw new RuntimeException("Failed : HTTP Error code : " + conn.getResponseCode());
        }

        conn.disconnect();
    }


    private String readLimited(InputStream in, int maxBytes) throws IOException
    {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int total = 0, n;
        while ((n = in.read(chunk)) != -1)
        {
            total += n;
            if (total > maxBytes)
            {
                throw new IOException("Response exceeds " + maxBytes + " bytes");
            }
            buf.write(chunk, 0, n);
        }
        return buf.toString(StandardCharsets.UTF_8);
    }

}
