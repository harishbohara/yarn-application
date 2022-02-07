package com.hb;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YarnSimpleLsCommandRunOnYarn {
    private static String appName = "harish";
    private static final String appMasterJarPath = "AppMaster.jar";

    public static void main(String[] args) throws IOException, YarnException, InterruptedException {

        // Step 1 - create a yarn clinet
        Configuration conf = new Configuration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        FileSystem fs = FileSystem.get(conf);
        yarnClient.start();

        // Step 2 - Create a app
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        System.out.println(appResponse);

        // Step 3 - Setup a application submission context i.e. what we want to do
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        appContext.setKeepContainersAcrossApplicationAttempts(true);
        appContext.setApplicationName(appName);
        appContext.setQueue("default");

        // Move this jar to Yarn so it can run it
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        addToLocalResources(
                fs,
                "/Users/harishbohara/workspace/personal/bigdata/target/bigdata-1.0-SNAPSHOT-jar-with-dependencies.jar",
                appMasterJarPath,
                appId.toString(),
                localResources,
                null
        );

        // What is the app priority
        Priority pri = Priority.newInstance(0);
        appContext.setPriority(pri);

        // Step 4 - what resources we need e.g. Ram and Cpu
        Resource capability = Resource.newInstance(1024, 1);
        appContext.setResource(capability);


        // What we want to do -> Here we just want to run a "ls -la" command
        Map<String, String> env = new HashMap<>();
        List<String> commands = new ArrayList<>();
        // commands.add("ls -la");
        commands.add("java -cp \"AppMaster.jar:.\" com.harish.yarn.am.ApplicationManager");

        // Step 5 - What we want to launch
        ContainerLaunchContext amContainer = ContainerLaunchContext
                .newInstance(
                        localResources, env, commands, null, null, null
                );
        appContext.setAMContainerSpec(amContainer);
        yarnClient.submitApplication(appContext);

        Thread.sleep(10_0000000);
        yarnClient.killApplication(appId);
    }

    private static void addToLocalResources(FileSystem fs, String fileSrcPath,
                                            String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                            String resources) throws IOException {
        String suffix =
                appName + "/" + appId + "/" + fileDstPath;
        Path dst =
                new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }
}
