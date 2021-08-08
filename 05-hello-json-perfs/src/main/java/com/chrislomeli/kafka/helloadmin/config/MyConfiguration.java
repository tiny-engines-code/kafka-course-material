package com.chrislomeli.kafka.helloadmin.config;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.*;

@Slf4j
public
class MyConfiguration {

    public static final String applicationId = "hello-world";

    public static List<String> servers;
    public static String topic;
    public static String token_url;
    public static String client_id;
    public static String client_secret;
    public static Boolean security = false;
    public static Boolean admin_client = false;
    public static Class<?> value_serializer;
    public static Class<?> value_deserializer;

    public static Properties globalProperties;

    private static void setGlobalProperties() {
        if (security) {
            assert (token_url != null);
            assert (client_id != null);
            assert (client_secret != null);
            String jassConfig = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                    String.format("oauth.token.endpoint.uri=\"%s\" ", token_url) +
                    String.format("oauth.client.id=\"%s\" ", client_id) +
                    String.format("oauth.client.secret=\"%s\" ;", client_secret);

            globalProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            globalProperties.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
            globalProperties.put(SaslConfigs.SASL_JAAS_CONFIG, jassConfig);
            globalProperties.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        }
    }


    public static void initializeConfiguration(String envPropertiesFile) {
        globalProperties = new Properties();
        setConfigurationFromFile(envPropertiesFile);
        setGlobalProperties();
        log.info("================================================================");
        log.info("    KAFKA_BOOTSTRAP == {}", servers);
        log.info("================================================================");

    }

    public static void setConfigurationFromFile(String envPropertiesFile) {
        Properties properties = new Properties();
        File f = new File(envPropertiesFile);
        if (!f.exists() || f.isDirectory()) {
            throw new RuntimeException(String.format("file %s does not exist", envPropertiesFile));
        }
        try (InputStream input = new FileInputStream(envPropertiesFile)) {
            properties.load(input);
            Class<?> c = MyConfiguration.class;
            Map<String, Field> fieldMap = new HashMap<>();
            Arrays.stream(c.getDeclaredFields()).forEach(x -> fieldMap.put(x.getName(), x));

            for (Object obj : properties.keySet()) {
                String key = (String) obj;
                String value = properties.getProperty(key);

                if (fieldMap.containsKey(key)) {

                    Field fld = fieldMap.get(key);
                    Class<?> typeOf = fld.getType();
                    fld.setAccessible(true);
                    try {
                        if (typeOf == List.class) {
                            fld.set(null, Arrays.asList(properties.getProperty("servers").split(",")));
                        } else if (typeOf == String.class) {
                            fld.set(null, value);
                        } else if (typeOf == Integer.class) {
                            fld.set(null, Integer.valueOf(value));
                        } else if (typeOf == Boolean.class) {
                            fld.set(null, "true".equals(value));
                        } else if (typeOf == Class.class) {
                            fld.set(null, Class.forName(value));
                        }
                    } catch (Exception ex) {
                        log.error("FUBAR", ex);
                    }

                }
            }

        } catch (IOException ex) {
            log.error("Configuration file error ", ex);
        }
    }
}
