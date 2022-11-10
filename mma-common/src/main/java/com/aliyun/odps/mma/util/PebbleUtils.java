package com.aliyun.odps.mma.util;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.loader.ClasspathLoader;
import com.mitchellbosecke.pebble.template.PebbleTemplateImpl;

public class PebbleUtils {
    public static String renderTpl(String templateFilePath, Map<String, Object> ctx) {
        PebbleEngine engine = new PebbleEngine.Builder().loader(new ClasspathLoader()).autoEscaping(false).build();

        PebbleTemplateImpl tpl = (PebbleTemplateImpl) engine.getTemplate(templateFilePath);
        try (Writer writer = new StringWriter()) {
            tpl.evaluate(writer, ctx);
            return writer.toString();
        } catch (IOException e) {
            return "error";
        }
    }
}
