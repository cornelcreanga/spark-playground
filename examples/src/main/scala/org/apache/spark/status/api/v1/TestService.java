package org.apache.spark.status.api.v1;

import org.apache.log4j.*;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Enumeration;

@Path("/custom")
public class TestService {

    @GET
    @Path("sum/{x}/{y}")
    @Produces({MediaType.APPLICATION_JSON})
    public int getApplicationList(
            @PathParam("x") int x,
            @PathParam("y") int y) {
        return x + y;
    }


    @GET
    @Path("loggers")
    public String getLoggers(){
        //jar:file:/home/cornel/.m2/repository/org/apache/spark/spark-core_2.12/3.3.0/spark-core_2.12-3.3.0.jar!/org/apache/spark/log4j2-defaults.properties
        //Logging.initializeLogging
        StringBuilder sb = new StringBuilder();

        Enumeration<Category> loggers = LogManager.getCurrentLoggers();
        while (loggers.hasMoreElements()) {
            Category category = loggers.nextElement();
            System.out.println(category.getClass() +" " + category.getName());
        }

        Enumeration e = Logger.getRootLogger().getAllAppenders();
        while ( e.hasMoreElements() ){
            Appender app = (Appender)e.nextElement();
            System.out.println(app.getClass() +" " + app.getName());
            if ( app instanceof FileAppender){
                System.out.println("File: " + ((FileAppender)app).getFile());
            }
        }
        return "Test";
    }

}
