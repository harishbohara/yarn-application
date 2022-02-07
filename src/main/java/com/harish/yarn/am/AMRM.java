package com.harish.yarn.am;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.util.List;
import java.util.Map;

public class AMRM extends AMRMClientAsync.AbstractCallbackHandler {
    private ApplicationAttemptId appAttemptID;
    private AMRMClientAsync<ContainerRequest> amrmClientAsync;

    public AMRM() {
        Map<String, String> envs = System.getenv();
        String containerIdString = envs.get("CONTAINER_ID");
        if (containerIdString == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException(
                    "ContainerId not set in the environment");
        }
        ContainerId containerId = ContainerId.fromString(containerIdString);
        appAttemptID = containerId.getApplicationAttemptId();
        System.out.println("ApplicationAttemptId=" + appAttemptID);
    }

    ContainerRequest setupContainerAskForRM() {
        // setup requirements for hosts
        // using * as any host will do for the distributed shell app
        // set the priority for the request
        Priority pri = Priority.newInstance(0);

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Resource.newInstance(256, 1);
        ContainerRequest request = new ContainerRequest(capability, null, null,
                pri);
        System.out.println("Requested container ask: " + request.toString());
        return request;
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        System.out.println("onContainersCompleted>>> ");
        for (ContainerStatus cs : statuses) {
            System.out.println(cs.getContainerId() + " ");
        }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        System.out.println("onContainersAllocated>>> ");
        for (Container c : containers) {
            System.out.println(c.getAllocationRequestId() + " " + c.toString());
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
}
