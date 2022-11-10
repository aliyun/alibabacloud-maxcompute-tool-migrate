package com.aliyun.odps.mma.validator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.*;

@Target({ ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER, ElementType.TYPE, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = {JsonValidator.class})
@Documented
public @interface ValidateJson {
    String message() default "invalid json parameter";

    JsonField[] fields() default {};
    Class<?>[] groups() default {};

    String configRequired() default "false";

    String[] require() default {};

    Class<? extends Payload>[] payload() default {};

    @Target({ ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER, ElementType.TYPE, ElementType.ANNOTATION_TYPE })
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public @interface List {
        ValidateJson[] value();
    }
}
