package com.creanga.playground.spark;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

//--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -agentpath:/home/cornel/async-profiler/build/libasyncProfiler.so=start,event=nativemem,timeout=20,file=/home/cornel/async-profiler/build/profile.html
//https://github.com/async-profiler/async-profiler/discussions/858
public class TestProfiler {

    private static class Inner1{
        private static void compressGzipFile(String file, String gzipFile) {
            try(FileInputStream fis = new FileInputStream(file);GZIPOutputStream gzipOS = new GZIPOutputStream(new FileOutputStream(gzipFile));) {
                byte[] buffer = new byte[4096];
                int len;
                while((len=fis.read(buffer)) != -1){
                    gzipOS.write(buffer, 0, len);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public static void main(String[] args) {
        System.out.println("Start");
        Inner1.compressGzipFile("/home/cornel/projects.tar","/home/cornel/projects.tar.gz");
        System.out.println("End");
    }


}
