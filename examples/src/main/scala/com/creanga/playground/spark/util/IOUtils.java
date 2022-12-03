package com.creanga.playground.spark.util;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

public class IOUtils {

    public static List<String> getResourceFileAsStream(String fileName) throws IOException {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        try (InputStream is = classLoader.getResourceAsStream(fileName)) {
            if (is == null) return null;
            try (InputStreamReader isr = new InputStreamReader(is);
                 BufferedReader reader = new BufferedReader(isr)) {
                return reader.lines().collect(Collectors.toList());
            }
        }
    }

    public static String alternatePaths(String... paths) throws URISyntaxException {
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];
            if (path.startsWith("file:/")){

                File file = new File(new URI(path));
                if (file.exists())
                    return path;
            }
        }
        throw new RuntimeException("no available paths");
    }

    public static void main(String[] args) throws URISyntaxException {
        System.out.println(alternatePaths("file:///home/cornel/parquet/",
                "s3://aws-roda-hcls-datalake/clinvar_summary_variants/hgvs4variation"));
    }

}
