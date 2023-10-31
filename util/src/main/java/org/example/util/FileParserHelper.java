package org.example.util;


import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** @author xiuzhu */
public class FileParserHelper {

    public static void main(String[] args) {
        String sql = "add file with /abc/aswx/cc.txt";
        File resourceFile = FileParserHelper.getResourceFile(sql);
        String resourceFileName = FileParserHelper.getResourceFileName(sql);

        String remoteFile = resourceFile.getAbsolutePath();
    }

    /**
     * todo: add java doc
     */
    private static final Pattern jarFilePattern =
            Pattern.compile("^(?!--)(?i)\\s*add\\s+jar\\s+with\\s+(\\S+)(\\s+AS\\s+(\\S+))?");

    /**
     * todo: add java doc
     */
    private static final Pattern resourceFilePattern =
            Pattern.compile("^(?!--)(?i)\\s*add\\s+file\\s+with\\s+(\\S+)?(\\s+rename\\s+(\\S+))?");

    /**
     * todo: add java doc
     */
    // engine用的文件匹配
    private static final Pattern engineFilePattern =
            Pattern.compile(
                    "^(?!--)(?i)\\s*add\\s+engine\\s+file\\s+with\\s+(\\S+)?(\\s+rename\\s+(\\S+))?(\\s+key\\s+(\\S+))?");

    /**
     * todo: add py doc
     */
    // Python 正则匹配
    private static final Pattern PYTHON_FILES_PATTERN =
            Pattern.compile("^(?!--)(?i)\\s*add\\s+python_files\\s+with\\s+([\\s\\S]*)");

    private static final Pattern PYTHON_REQUIREMENTS_PATTERN =
            Pattern.compile(
                    "^(?!--)(?i)\\s*add\\s+python_requirements\\s+with\\s+(\\S+)?(\\s+rename\\s+(\\S+))?");

    private static final Pattern PYTHON_DEPENDENCIES_PATTERN =
            Pattern.compile("^(?!--)(?i)\\s*add\\s+python_dependencies\\s+with\\s+([\\s\\S]*)");

    private static final Pattern REALLY_PYTHON_FILE_PATTERN =
            Pattern.compile("(?i)(\\S+)\\s+rename\\s+(\\S+)");

    private static final Pattern CATALOG_PATTERN =
            Pattern.compile("(?i)\\s*CREATE\\s+CATALOG\\s+(\\S+)\\s+WITH((.|\\n)+)");


    public static boolean verifyJar(String sql) {
        return jarFilePattern.matcher(sql).find();
    }

    public static File getResourceFile(String sql) {
        Matcher matcher = resourceFilePattern.matcher(sql);
        if (!matcher.find()) {
            throw new RuntimeException("Get Resource File Error: " + sql);
        }
        String filePath = matcher.group(1);
        return new File(filePath);
    }

    public static String getResourceFileName(String sql) {
        Matcher matcher = resourceFilePattern.matcher(sql);
        if (!matcher.find()) {
            throw new RuntimeException("Get Resource File Name Error: " + sql);
        }

        String fileName = matcher.group(3);
        if (StringUtils.isBlank(fileName)) {
            fileName = getResourceFile(sql).getName();
        }
        return fileName;
    }

    public static boolean verifyResource(String sql) {
        return resourceFilePattern.matcher(sql).find();
    }

    /**
     * handle add jar statements and comment statements on the same line " --desc \n\n ADD JAR WITH
     * xxxx"
     */
    public static String handleSql(String sql) {
        String[] sqls = sql.split("\\n");
        for (String s : sqls) {
            if (verifyJar(s)) {
                return s;
            }

            if (verifyResource(s)) {
                return s;
            }
        }
        return sql;
    }

    public static boolean verifyPythonFiles(String sql) {
        return PYTHON_FILES_PATTERN.matcher(sql).find();
    }


    public static boolean verifyPythonRequirements(String sql) {
        return PYTHON_REQUIREMENTS_PATTERN.matcher(sql).find();
    }


    public static boolean verifyCatalogPattern(String sql) {
        return CATALOG_PATTERN.matcher(sql).find();
    }

    public static String parsePythonFileReallyName(String sql) {
        Matcher matcher = REALLY_PYTHON_FILE_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new RuntimeException(
                    "not a rename operator, don't obtain a really file name:" + sql);
        }
        return matcher.group(1);
    }

    public static boolean verifyEngineFile(String sql) {
        return engineFilePattern.matcher(sql).find();
    }

}