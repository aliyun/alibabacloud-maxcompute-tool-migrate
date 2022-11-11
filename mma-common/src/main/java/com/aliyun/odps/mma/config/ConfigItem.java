package com.aliyun.odps.mma.config;

import java.lang.annotation.*;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ConfigItem {
    String desc();
    String type() default "string";
    String defaultValue() default "";
    boolean required() default false;
}