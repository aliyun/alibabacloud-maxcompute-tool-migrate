package com.aliyun.odps.mma.advice;

import com.aliyun.odps.mma.api.ApiRes;
import com.aliyun.odps.mma.execption.JobConfigException;
import com.aliyun.odps.mma.execption.MMAException;
import org.springframework.context.support.DefaultMessageSourceResolvable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import java.util.*;
import java.util.stream.Collectors;

@RestControllerAdvice
public class ControllerAspect {
    @ExceptionHandler(ConstraintViolationException.class)
    public ApiRes constraintViolationException(ConstraintViolationException e) {
        Set<ConstraintViolation<?>> constraintViolations = e.getConstraintViolations();

        List<String> errors = constraintViolations.stream()
                .map(ConstraintViolation::getMessage)
                .collect(Collectors.toList());

        String error = errors.get(0);
        Optional<ApiRes> apiResOpt = ApiRes.tryFromJson(error);
        if (apiResOpt.isPresent()) {
            return apiResOpt.get();
        }

        ApiRes apiRes = new ApiRes();
        apiRes.setMessage(error);
        return apiRes;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ApiRes methodArgumentNotValidExceptionHandler(MethodArgumentNotValidException e) {
        List<FieldError> fieldErrors = e.getBindingResult().getFieldErrors();

        List<String> errors = fieldErrors.stream()
                .map(DefaultMessageSourceResolvable::getDefaultMessage)
                .collect(Collectors.toList());

        String error = errors.get(0);
        Optional<ApiRes> apiResOpt = ApiRes.tryFromJson(error);
        if (apiResOpt.isPresent()) {
            return  apiResOpt.get();
        }

        ApiRes apiRes = new ApiRes();
        apiRes.setMessage(error);
        return apiRes;
    }

    @ExceptionHandler(MMAException.class)
    public ApiRes mmaExceptionHandler(MMAException exception) {
        ApiRes apiRes = new ApiRes();

        if (exception instanceof JobConfigException) {
            JobConfigException je = (JobConfigException) exception;
            apiRes.addError(je.getField(), je.getErrMsg());
        } else {
            String error = exception.getMessage();
            apiRes.setMessage(error);
        }

        return apiRes;
    }
}
