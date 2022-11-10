package com.aliyun.odps.mma;

import com.aliyun.odps.mma.util.ExceptionUtils;
import com.beust.jcommander.*;
import com.beust.jcommander.converters.FileConverter;
import org.ini4j.Ini;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CmdArgParser {
    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;

    @Parameter(names = {"-c", "---config"}, required = true, converter = FileConverter.class, validateValueWith = FileValidator.class)
    private File configFile;

    public void parse(String[] args) {
        JCommander jc = JCommander.newBuilder()
                .addObject(this)
                .build();

        if (this.help) {
            jc.usage();
            System.exit(0);
        }

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        parseConfigFile();
    }

    public void parseConfigFile() {
        String[] sections = new String[] {"mysql", "mma"};
        String[][] optionsOfSection = new String[][]{
                {"host", "port", "db", "username", "password"},
                {"listening_port", "sch_rate", "task_max_num", "debug"}
        };

        Map<String, String> optionToProperty = new HashMap<String, String>() {{
            put("host", "MYSQL_HOST");
            put("port", "MYSQL_PORT");
            put("db", "MYSQL_DB");
            put("username", "MYSQL_USER");
            put("password", "MYSQL_PASS");
            put("listening_port", "APP_PORT");
            put("sch_rate", "SCH_RATE");
            put("debug", "MMA_DEBUG");
        }};

        try {
            Ini ini = new Ini(configFile);

            for (int i = 0, n = sections.length; i < n; i ++) {
                String section = sections[i];
                String[] options = optionsOfSection[i];

                for (String option: options) {
                    String value = ini.get(section, option);

                    if (Objects.isNull(value)) {
                        value = "";
                    }
                    String property = optionToProperty.get(option);

                    if (Objects.nonNull(property) && !"".equals(value)) {
                        System.setProperty(property, value);
                    }
                }
            }

            String debug = ini.get("mma", "debug");
            if (Objects.equals(debug, "true")) {
                System.setProperty("PROFILE", "dev");
            }

            String taskMaxNumStr = ini.get("mma", "task_max_num");
            if (Objects.nonNull(taskMaxNumStr)) {
                try {
                    int taskMaxNum = Integer.parseInt(taskMaxNumStr);
                    System.setProperty("TASK_MAX_NUM", taskMaxNumStr);
                    System.setProperty("DB_POOL_SIZE", Integer.toString(taskMaxNum + 20));
                } catch (Exception e) {
                    throw new RuntimeException("mma.task_max_num must be a number, now it is " + taskMaxNumStr);
                }
            }
        } catch (IOException e) {
            System.err.printf("read %s failed with error msg %s\n", configFile.getPath(), ExceptionUtils.getStackTrace(e));
            System.exit(1);
        }
    }

    public static class FileValidator implements IValueValidator<File> {
        @Override
        public void validate(String name, File value) throws ParameterException {
            if (! value.isFile()) {
                throw new ParameterException(
                        String.format("config file %s doest not existed", value.getAbsolutePath())
                );
            }
        }
    }
}
