package com.hadoop.yh.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Client {

    public static void main(String[] args) throws IOException, YarnException {
        String rmAddress;
        Configuration conf = new Configuration();
        ApplicationClientProtocol rmClient = (ApplicationClientProtocol) RPC.getProxy(ApplicationClientProtocol.class,
                1, new InetSocketAddress(8080)
                , conf);
        //构造一个可序列化对象
        GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
        //返回一个ApplicationId 以及最大可申请资源数量
        GetNewApplicationResponse newApplication = rmClient.getNewApplication(request);

        ApplicationId applicationId = newApplication.getApplicationId();


        ApplicationSubmissionContext applicationSubmissionContext = Records.newRecord(ApplicationSubmissionContext.class);
        //设置应用程序名称 还可以设置优先级 队列名称
        applicationSubmissionContext.setApplicationName("nameq");

        //构建一个M启动上下文对象
        ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
        //设置AM启动所需的本地资源
        containerLaunchContext.setLocalResources(Collections.emptyMap());
        //设置AM启动所需的环境变量
        containerLaunchContext.setEnvironment(Collections.EMPTY_MAP);

        applicationSubmissionContext.setAMContainerSpec(containerLaunchContext);
        applicationSubmissionContext.setApplicationId(applicationId);

        SubmitApplicationRequest submitApplicationRequest = Records.newRecord(SubmitApplicationRequest.class);
        submitApplicationRequest.setApplicationSubmissionContext(applicationSubmissionContext);

        rmClient.submitApplication(submitApplicationRequest);


        //YARN封装的一套客户端
        YarnClient client = YarnClient.createYarnClient();
        client.init(new Configuration());
        client.start();
        YarnClientApplication application = client.createApplication();
        ApplicationSubmissionContext context = application.getApplicationSubmissionContext();
        ApplicationId applicationId1 = context.getApplicationId();
        context.setApplicationName("name1");
        client.submitApplication(context);

        //RM交互部分
        MyCallbackHandler allocListener = new MyCallbackHandler();
        //构建一个AMRMClientAsync句柄
        AMRMClientAsync<AMRMClient.ContainerRequest> asyncClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        //通过传入一个YarnConfiguration对象进行初始化
        asyncClient.init(new Configuration());
        //启动asyncClient
        asyncClient.start();
        //ApplicationMaster 向RM注册
        RegisterApplicationMasterResponse response = asyncClient.registerApplicationMaster("localhost", 8081, "http://");
        //添加Container请求
        asyncClient.addContainerRequest(AMRMClient.ContainerRequest.newBuilder().build());
        //等待应用程序结束
        asyncClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "appMsg", null);
        asyncClient.stop();

      //NM交互部分
        NMClientAsyncImpl nmClientAsync = new NMClientAsyncImpl(new MyNmCallbackHandler());
        nmClientAsync.init(new Configuration());
        nmClientAsync.start();
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
//        nmClientAsync.startContainerAsync(Container.newInstance(),ctx);
//        nmClientAsync.getContainerStatusAsync(container.getid());
//        nmClientAsync.stopContainerAsync(container.getId(),container.getNodeiD(),container.getContainerToken());
        nmClientAsync.stop();
    }



    static class MyCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
        /**
         * 被调用时机 RM为AM返回的心跳应答中包含完成的Container信息
         * 如果心跳应答中同时包含完成的Container 和 新分配的Container 该回调函数会在 onContainersAllocated 之前调用
         * @param list
         */
        @Override
        public void onContainersCompleted(List<ContainerStatus> list) {

        }

        /**
         * RM为AM返回的心跳包包含新分配的Container信息
         * 如果同时包含完成的Container 和 新分配的Container 会在 onContainersCompleted 之前回调
         * @param list
         */
        @Override
        public void onContainersAllocated(List<Container> list) {

        }

        @Override
        public void onContainersUpdated(List<UpdatedContainer> list) {

        }

        /**
         * RM通知AM停止运行
         */
        @Override
        public void onShutdownRequest() {

        }

        /**
         * RM管理的节点发生变化 比如健康节点变得不健康 节点不可用
         * @param list
         */
        @Override
        public void onNodesUpdated(List<NodeReport> list) {

        }

        @Override
        public float getProgress() {
            return 0;
        }

        /**
         * 任何出现异常
         * @param throwable
         */
        @Override
        public void onError(Throwable throwable) {

        }
    }

    static class MyNmCallbackHandler extends NMClientAsync.AbstractCallbackHandler{
        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {

        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

        }

        @Override
        public void onContainerStopped(ContainerId containerId) {

        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable throwable) {

        }

        @Override
        public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {

        }

        @Override
        public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {

        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {

        }

        @Override
        public void onIncreaseContainerResourceError(ContainerId containerId, Throwable throwable) {

        }

        @Override
        public void onUpdateContainerResourceError(ContainerId containerId, Throwable throwable) {

        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable throwable) {

        }
    }

}
