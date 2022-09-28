package com.creanga.playground.spark.example.rest;

import org.apache.log4j.LogManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/custom")
public class RestApi {

    @GET
    @Path("loggers/{logger}")
    @Produces({MediaType.APPLICATION_JSON})
    public String getApplicationList(@PathParam("logger") String logger) {
        return LogManager.getLogger(logger).getLevel().toString();
    }

}
