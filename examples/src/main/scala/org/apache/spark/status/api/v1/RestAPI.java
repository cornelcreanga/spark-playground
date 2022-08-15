package org.apache.spark.status.api.v1;



import org.apache.spark.SparkContext;
import org.apache.spark.ui.SparkUI;
//import org.eclipse.jetty.servlet.*;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.sparkproject.jetty.servlet.ServletContextHandler;
import org.sparkproject.jetty.servlet.ServletHolder;


public class RestAPI{

    public ServletContextHandler getServletHandler(){
        ServletContextHandler  jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        jerseyContext.setContextPath("/rest");
        ServletHolder holder = new ServletHolder(ServletContainer.class);
        holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "org.apache.spark.rest.services");
        jerseyContext.addServlet(holder, "/*");
        return jerseyContext;
    }

    public static SparkUI getSparkUI(SparkContext sparkContext){
        return sparkContext.ui().getOrElse(() -> {
                throw new RuntimeException("Parent SparkUI to attach this tab to not found!");
        });
    }

    public static void apply(SparkContext sparkContext){
        attach(sparkContext);
    }

    public static void attach(SparkContext sparkContext){
        ServletContextHandler handler = new RestAPI().getServletHandler();
        getSparkUI(sparkContext).attachHandler(handler);
        System.out.println("start");
    }

    public static void detach(SparkContext sparkContext){
        getSparkUI(sparkContext).detachHandler(new RestAPI().getServletHandler());
        System.out.println("stop");
    }

}
