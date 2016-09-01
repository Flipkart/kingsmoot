package com.flipkart.etcd;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public final class JsonUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        OBJECT_MAPPER.setSerializationInclusion(Include.NON_NULL);
        OBJECT_MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ"));
        OBJECT_MAPPER.setTimeZone(TimeZone.getTimeZone("Asia/Calcutta"));
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true);
        OBJECT_MAPPER.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    }


    public static final <T> T deserializeJson(String json, Class<T> type)
            throws JsonParseException, JsonMappingException, IOException {
        return OBJECT_MAPPER.readValue(json, type);
    }

    public static final <T> T deserializeJson(String json, TypeReference<T> valueTypeRef)
            throws JsonParseException, JsonMappingException, IOException {
        return OBJECT_MAPPER.readValue(json, valueTypeRef);
    }

    public static final String serializeJson(Object object) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(object);
    }

}
