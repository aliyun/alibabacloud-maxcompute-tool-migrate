package com.aliyun.odps.mma.validator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.FIELD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface JsonField {
    String key();
    boolean required() default false;
    String requiredIfExist() default "";
    Class<?> type() default String.class;
    Class<?> elemType() default String.class;
}
