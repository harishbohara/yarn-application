package com.harish.yarn.am;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class ApplicationManager {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new YarnConfiguration();

        // Step 1 - setup Application manager <> Resource manager
        AMRM amrm = new AMRM();
        AMRMClientAsync<ContainerRequest> amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(1000, amrm);
        amrmClientAsync.init(configuration);
        amrmClientAsync.start();

        // Step 2 - setup node manager handler
        NMClientAsync.AbstractCallbackHandler callbackHandler;
        NMClientAsync nmClientAsync = new NMClientAsyncImpl(callbackHandler = new NMListener());
        nmClientAsync.init(configuration);
        nmClientAsync.start();

        // Pass these to "application manager  - resource manager" handler
        amrm.setNMClientAsync(nmClientAsync, callbackHandler);

        // Step 3 - (Mandatory - otherwise you will not get the allocations)
        // Register health check wit hRM
        String appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amrmClientAsync.registerApplicationMaster(appMasterHostname, -1, "");
        int maxMem = response.getMaximumResourceCapability().getMemory();
        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        System.out.println("Max mem/cpu capability of resources in this cluster " + maxMem + "/" + maxVCores);

        // Step 4 - Ask for few containers - application manger says it wants to have 3 nodes.
        for (int i = 0; i < 3; ++i) {
            ContainerRequest containerAsk = setupContainerAskForRM();
            amrmClientAsync.addContainerRequest(containerAsk);
        }

        for (int i = 0; i < 1000; i++) {
            System.out.println("->>> we just loop to keep the application master running " + i + "  appMasterHostname=" + appMasterHostname);
            Thread.sleep(5000);
        }
    }

    // Build a request for container
    static ContainerRequest setupContainerAskForRM() {
        Priority pri = Priority.newInstance(0);
        Resource capability = Resource.newInstance(256, 1);
        ContainerRequest request = new ContainerRequest(capability, null, null, pri);
        System.out.println("Requested container ask: " + request);
        return request;
    }
}
