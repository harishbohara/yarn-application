package com.harish.yarn.am;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class ApplicationManager {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new YarnConfiguration();

        // Step 1 - setup Application manager <> Resource manager
        AMRM amrm = new AMRM();
        AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(1000, amrm);
        amrmClientAsync.init(configuration);
        amrmClientAsync.start();

        // Step 2 - setup node manager handler
        NMClientAsync nmClientAsync = new NMClientAsyncImpl(new NMListener());
        nmClientAsync.init(configuration);
        nmClientAsync.start();

        // Step 3 - (Mandatory - otherwise you will not get the allocations)
        // Register health check wit hRM
        String appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amrmClientAsync.registerApplicationMaster(appMasterHostname, -1, "");
        int maxMem = response.getMaximumResourceCapability().getMemory();
        System.out.println("Max mem capabililty of resources in this cluster " + maxMem);
        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        System.out.println("Max vcores capabililty of resources in this cluster " + maxVCores);

        // Step 4 - Ask for few containers
        for (int i = 0; i < 3; ++i) {
            AMRMClient.ContainerRequest containerAsk = amrm.setupContainerAskForRM();
            amrmClientAsync.addContainerRequest(containerAsk);
        }

        for (int i = 0; i < 1000; i++) {
            System.out.println("->>> " + i + "  appMasterHostname=" + appMasterHostname);
            Thread.sleep(1000);
        }
    }
}
