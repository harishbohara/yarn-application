package com.harish.yarn.am;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class AMRM extends AMRMClientAsync.AbstractCallbackHandler {
    private AMRMClientAsync<ContainerRequest> amrmClientAsync;
    private NMClientAsync nmClientAsync;
    public volatile boolean stop = false;

    public AMRM() {

        // Print all env var available to us
        Map<String, String> envs = System.getenv();
        envs.forEach((s, s2) -> {
            System.out.println("-->> key=" + s + " value=" + s2);
        });

        // Not mandatory - some logs to print
        String containerIdString = envs.get("CONTAINER_ID");
        if (containerIdString == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException("ContainerId not set in the environment");
        }
        ContainerId containerId = ContainerId.fromString(containerIdString);
        ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
        System.out.println("ApplicationAttemptId=" + appAttemptID);

        // We are sending it form the application master
        String appId = envs.get("__app_id__");
        System.out.println("ApplicationId=" + appId);
    }

    public void setNMClientAsync(NMClientAsync nmClientAsync, NMClientAsync.AbstractCallbackHandler callbackHandler) {
        this.nmClientAsync = nmClientAsync;
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        System.out.println("onContainersCompleted>>> ");
        for (ContainerStatus cs : statuses) {
            System.out.println(cs.getContainerId() + " " + cs.toString());
        }
        stop = true;
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        System.out.println("onContainersAllocated>>> ");
        for (Container c : containers) {
            System.out.println(c.getAllocationRequestId() + " " + c);

            // We got container - lets run it
            new Thread(() -> {
                try {
                    setupContainers(c);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    @Override
    public void onContainersUpdated(List<UpdatedContainer> containers) {
        System.out.println("onContainersUpdated>>> ");
        for (UpdatedContainer c : containers) {
            System.out.println(c.getContainer());
        }
    }

    @Override
    public void onShutdownRequest() {
        System.out.println("onShutdownRequest>>> ");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        System.out.println("onContainersUpdated>>> ");
        for (NodeReport c : updatedNodes) {
            System.out.println(c.toString());
        }
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable e) {
        System.out.println("onError>>> ");
        e.printStackTrace();
    }

    public void setupContainers(Container container) throws IOException {
        // Set the necessary command to execute on the allocated container
        List<String> commands = new Vector<String>(5);
        commands.add("ls -la; ");
        commands.add("java -cp \"AppMaster.jar:.\" com.harish.yarn.application.Application");

        // V-IMP - this is very important - we need to send the application jar to nodes
        // Here i have all code in AppMaster.jar so I am sending it to all nodes
        // We have this AppMaster.jar which we will send to all nodes
        String packagePath = "AppMaster.jar";
        File packageFile = new File(packagePath);
        URL packageUrl = ConverterUtils.getYarnUrlFromPath(
                FileContext.getFileContext().makeQualified(new Path(packagePath)));
        System.out.println("--->> packageFile=" + packageFile);
        System.out.println("--->> packageUrl=" + packageUrl);

        LocalResource packageResource = LocalResource.newInstance(
                packageUrl,
                LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION,
                packageFile.length(),
                packageFile.lastModified()
        );

        // Move this jar to Yarn so it can run it
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        Map<String, String> env = new HashMap<>();
        ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);
        ctx.setLocalResources(Collections.singletonMap("AppMaster.jar", packageResource));

        // Start a container
        nmClientAsync.startContainerAsync(container, ctx);
    }
}
