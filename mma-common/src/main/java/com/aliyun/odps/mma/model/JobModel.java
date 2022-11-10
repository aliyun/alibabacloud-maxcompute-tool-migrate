package com.aliyun.odps.mma.model;

import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.constant.JobStatus;
import com.aliyun.odps.mma.constant.JobType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import lombok.Data;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;


@Data
//@JsonSerialize(using = JobModel.JobSerializer.class)
@JsonDeserialize(using = JobModel.JonDeserializer.class)
public class JobModel {
    private Integer id;
    private String description;
    @JsonProperty("source_name")
    String sourceName;
    @JsonProperty("db_name")
    String dbName;
    @JsonProperty("dst_mc_project")
    String dstOdpsProject;
    private JobStatus status = JobStatus.INIT;
    private JobType type;
    private Boolean stopped;
    private Boolean restart;
    private Boolean deleted;
    private JobConfig config;
    private Date createTime;
    private Date updateTime;

    // 非数据库字段
    @JsonIgnore
    private List<String> tableNames;

    public static class JobSerializer extends StdSerializer<JobModel> {
        protected JobSerializer() {
            super(JobModel.class);
        }

        @Override
        public void serialize(JobModel jobModel, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeStartObject();

            for (String fieldName : JobModelInfo.fieldMap.keySet()) {
                Field field = JobModelInfo.fieldMap.get(fieldName);
                if (Objects.nonNull(field.getAnnotation(JsonIgnore.class))) {
                    continue;
                }

                if (fieldName.equals("config")) {
                    continue;
                }

                try {
                    Object value = field.get(jobModel);

                    if (Objects.nonNull(value)) {
                        jsonGenerator.writeObjectField(fieldName, value);
                    }
                } catch (IllegalAccessException _e) {
                    // unreachable!
                }
            }

            ObjectMapper om = new ObjectMapper();
            JsonNode configTree = om.valueToTree(jobModel.config);
            Iterator<Map.Entry<String, JsonNode>> nodes = configTree.fields();
            while (nodes.hasNext()) {
                Map.Entry<String, JsonNode> node = nodes.next();
                jsonGenerator.writeFieldName(node.getKey());
                jsonGenerator.writeTree(node.getValue());
            }

            jsonGenerator.writeEndObject();
            //jsonGenerator.close();
        }
    }

    public static class JonDeserializer extends StdDeserializer<JobModel> {
        protected JonDeserializer() {
            super(JobModel.class);
        }

        @Override
        public JobModel deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            JsonNode tree = jsonParser.getCodec().readTree(jsonParser);
            Iterator<String> jsonFields = tree.fieldNames();
            JobModel jobModel = new JobModel();
            ObjectMapper om = new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);


            while (jsonFields.hasNext()) {
                String jsonField = jsonFields.next();
                JsonNode jsonNode = tree.get(jsonField);
                Field reflectField = JobModelInfo.fieldMap.get(jsonField);

                if (Objects.isNull(reflectField)) {
                    continue;
                }

                try {
                    reflectField.set(jobModel, om.treeToValue(jsonNode, reflectField.getType()));
                } catch (IllegalAccessException e) {
                    // !unreachable
                }
            }

            jobModel.config = om.treeToValue(tree, JobConfig.class);
            return jobModel;
        }
    }

    public static class JobModelInfo {
        public static final Map<String, Field> fieldMap = new HashMap<>();

        static {
            Class<JobModel> c = JobModel.class;
            for (Field f: c.getDeclaredFields()) {
                f.setAccessible(true);
                JsonProperty jp = f.getAnnotation(JsonProperty.class);
                if (Objects.nonNull(jp)) {
                    fieldMap.put(jp.value(), f);
                } else {
                    fieldMap.put(f.getName(), f);
                }
            }
        }

        public static boolean isJobModelField(String name) {
            return fieldMap.containsKey(name);
        }
    }
}
