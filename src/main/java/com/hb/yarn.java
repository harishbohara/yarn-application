package com.hb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class yarn {
    public static void main(String[] args) throws IOException, YarnException, InterruptedException {

        // Step 1 - create a yarn clinet
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(new Configuration());
        yarnClient.start();

        // Step 2 - Create a app
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        System.out.println(appResponse);

        // Step 3 - Setup a application submission context i.e. what we want to do
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        appContext.setKeepContainersAcrossApplicationAttempts(true);
        appContext.setApplicationName("harish");
        appContext.setQueue("default");

        // What is the app priority
        Priority pri = Priority.newInstance(0);
        appContext.setPriority(pri);

        // Step 4 - what resources we need e.g. Ram and Cpu
        Resource capability = Resource.newInstance(1024, 2);
        appContext.setResource(capability);


        // What we want to do -> Here we just want to run a "ls -la" command
        Map<String, String> env = new HashMap<>();
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        List<String> commands = new ArrayList<>();
        commands.add("ls -la");

        // Step 5 - What we want to launch
        ContainerLaunchContext amContainer = ContainerLaunchContext
                .newInstance(
                        localResources, env, commands, null, null, null
                );
        appContext.setAMContainerSpec(amContainer);
        yarnClient.submitApplication(appContext);

        Thread.sleep(10_000);
        yarnClient.killApplication(appId);
    }
}
