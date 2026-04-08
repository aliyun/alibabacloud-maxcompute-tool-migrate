package com.aliyun.odps.mma.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.context.NoSuchMessageException;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.stereotype.Component;

import java.util.Locale;

@Component
public class I18nUtils {
    private final MessageSource messageSource;

    @Autowired
    public I18nUtils(MessageSource messageSource) {
        this.messageSource = messageSource;
    }

    public String get(String msgKey, String langCode, String defaultMsg) {
        Locale locale = toLocale(langCode);

        return get(msgKey, locale, defaultMsg);
    }

    public String get(String msgKey, String defaultMsg) {
        Locale locale = LocaleContextHolder.getLocale();
        return get(msgKey, locale, defaultMsg);
    }

    public String get(String msgKey, Locale locale, String defaultMsg) {
        try {
            return messageSource.getMessage(msgKey, null, locale);
        } catch (NoSuchMessageException e) {
            return defaultMsg;
        }
    }

    private Locale toLocale(String code) {
        if (StringUtils.isBlank(code)) {
            return new Locale("zh", "CN");
        }
        String[] splitters = new String[] {"_", "-"};

        for (String splitter: splitters) {
            String[] langAndCountry = code.split(splitter);

            if (langAndCountry.length == 2) {
                return new Locale(langAndCountry[0], langAndCountry[1]);
            }
        }

        return new Locale("zh", "CN");
    }
}
