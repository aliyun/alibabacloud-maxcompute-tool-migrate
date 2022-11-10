package com.aliyun.odps.mma.validator;

import com.aliyun.odps.mma.api.ApiRes;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.*;
import java.util.stream.Collectors;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class JsonValidator implements ConstraintValidator<ValidateJson, Map<String, Object>> {
    private final Map<String, JsonField> fieldMap = new HashMap<>();
    private final ApiRes apiRes = new ApiRes();

    @Override
    public void initialize(ValidateJson constraintAnnotation) {
        for (JsonField jsonField: constraintAnnotation.fields()) {
            fieldMap.put(jsonField.key(), jsonField);
        }

        apiRes.setMessage(constraintAnnotation.message());
    }

    @Override
    public boolean isValid(Map<String, Object> rawMap, ConstraintValidatorContext context) {
        context.disableDefaultConstraintViolation();

        boolean isOk = true;

        for (String key: fieldMap.keySet()) {
            JsonField jsonField = fieldMap.get(key);
            if (jsonField.required() && !rawMap.containsKey(key)) {
                apiRes.addError(key, "is required");
                isOk = false;
                continue;
            }

            if (!rawMap.containsKey(key)) {
                continue;
            }

            Object value = rawMap.get(key);
            Class fieldType = jsonField.type();

            if (! fieldType.isEnum()) {
                String expectedType = fieldType.getSimpleName();
                String realType = value.getClass().getSimpleName();

               if (! expectedType.equals(realType)) {
                   isOk = false;
                   apiRes.addError(key, String.format("should be %s type, but get %s type", expectedType, realType));
               }
            } else {
                if (!(value instanceof String)) {
                    isOk = false;
                    apiRes.addError(key, "is not a string");
                } else {
                    List<String> allValues = Arrays
                            .stream(fieldType.getEnumConstants())
                            .map(Object::toString)
                            .map(String::toLowerCase)
                            .collect(Collectors.toList());

                    String valueStr = (String) value;
                    if (! allValues.contains(valueStr.toLowerCase())) {
                        isOk = false;
                        apiRes.addError(key, String.format("should be one of (%s)", String.join(", ", allValues)));
                    }
                }
            }
        }

        context.buildConstraintViolationWithTemplate(apiRes.toJson()).addConstraintViolation();

        return isOk;
    }
}
