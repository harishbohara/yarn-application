package com.harish.yarn.am;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.Map;

public class NMListener extends NMClientAsync.AbstractCallbackHandler {
    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {

    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

    }

    @Override
    public void onContainerStopped(ContainerId containerId) {

    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {

    }

    @Override
    public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {

    }

    @Override
    public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {

    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {

    }

    @Override
    public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) {

    }

    @Override
    public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) {

    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {

    }
}
