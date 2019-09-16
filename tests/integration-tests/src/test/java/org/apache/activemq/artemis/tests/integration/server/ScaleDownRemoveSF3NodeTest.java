/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.server;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.core.config.ScaleDownConfiguration;
import org.apache.activemq.artemis.core.config.ha.LiveOnlyPolicyConfiguration;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.AddressQueryResult;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.cluster.impl.ClusterConnectionImpl;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(value = Parameterized.class)
public class ScaleDownRemoveSF3NodeTest extends ClusterTestBase {

   @Parameterized.Parameters(name = "RemoveOption={0}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{"default"}, {"true"}, {"false"}});
   }

   public ScaleDownRemoveSF3NodeTest(String option) {
      this.option = option;
   }

   private String option;

   String node0Id;
   String node1Id;
   String node2Id;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      ScaleDownConfiguration scaleDownConfiguration = new ScaleDownConfiguration();
      if (!"default".equals(option)) {
         scaleDownConfiguration.setCleanupSfQueue("true".equals(this.option));
      }

      setupLiveServer(0, isFileStorage(), false, true, true);
      servers[0].getConfiguration().setSecurityEnabled(false);
      setupLiveServer(1, isFileStorage(), false, true, true);
      servers[1].getConfiguration().setSecurityEnabled(false);
      setupLiveServer(2, isFileStorage(), false, true, true);
      servers[2].getConfiguration().setSecurityEnabled(false);

      //server 0 is the scale down broker
      LiveOnlyPolicyConfiguration haPolicyConfiguration0 = (LiveOnlyPolicyConfiguration) servers[0].getConfiguration().getHAPolicyConfiguration();
      haPolicyConfiguration0.setScaleDownConfiguration(scaleDownConfiguration);

      LiveOnlyPolicyConfiguration haPolicyConfiguration1 = (LiveOnlyPolicyConfiguration) servers[1].getConfiguration().getHAPolicyConfiguration();
      ScaleDownConfiguration scaleDownConfiguration1 = new ScaleDownConfiguration();
      haPolicyConfiguration1.setScaleDownConfiguration(scaleDownConfiguration1);

      LiveOnlyPolicyConfiguration haPolicyConfiguration2 = (LiveOnlyPolicyConfiguration) servers[2].getConfiguration().getHAPolicyConfiguration();
      ScaleDownConfiguration scaleDownConfiguration2 = new ScaleDownConfiguration();
      haPolicyConfiguration2.setScaleDownConfiguration(scaleDownConfiguration2);

      setupClusterConnection("cluster0", "testAddress", MessageLoadBalancingType.ON_DEMAND, 1, true, 0, 1, 2);
      setupClusterConnection("cluster0", "testAddress", MessageLoadBalancingType.ON_DEMAND, 1, true, 1, 0, 2);
      setupClusterConnection("cluster0", "testAddress", MessageLoadBalancingType.ON_DEMAND, 1, true, 2, 0, 1);

      //make sure server0 scaled down to server1
      scaleDownConfiguration.setGroupName("bill");
      scaleDownConfiguration1.setGroupName("bill");
      scaleDownConfiguration1.setEnabled(false);

      String targetConnector = servers[0].getConfiguration().getClusterConfigurations().get(0).getStaticConnectors().get(0);
      Assert.assertEquals(61617, servers[0].getConfiguration().getConnectorConfigurations().get(targetConnector).getParams().get(TransportConstants.PORT_PROP_NAME));
      haPolicyConfiguration0.getScaleDownConfiguration().getConnectors().add(targetConnector);

      startServers(0, 1, 2);

      setupSessionFactory(0, true, false, servers[0].getConfiguration().getClusterUser(), servers[0].getConfiguration().getClusterPassword());
      setupSessionFactory(1, true, false, servers[1].getConfiguration().getClusterUser(), servers[1].getConfiguration().getClusterPassword());
      setupSessionFactory(2, true, false, servers[2].getConfiguration().getClusterUser(), servers[2].getConfiguration().getClusterPassword());

      node0Id = servers[0].getClusterManager().getNodeId();
      node1Id = servers[1].getClusterManager().getNodeId();
      node2Id = servers[2].getClusterManager().getNodeId();

      IntegrationTestLogger.LOGGER.info("===============================");
      IntegrationTestLogger.LOGGER.info("Node 0: " + node0Id);
      IntegrationTestLogger.LOGGER.info("Node 1: " + node1Id);
      IntegrationTestLogger.LOGGER.info("Node 2: " + node2Id);
      IntegrationTestLogger.LOGGER.info("===============================");
   }


   @Test
   public void testScaleDownCheckSF() throws Exception {

      final int TEST_SIZE = 2;
      final String addressName = "testAddress";
      final String queueName1 = "testQueue1";

      // create 3 queues on each node mapped to the same address
      createQueue(0, addressName, queueName1, null, true);
      createQueue(1, addressName, queueName1, null, true);
      createQueue(2, addressName, queueName1, null, true);

      waitForBindings(0, addressName, 1, 0, true);
      waitForBindings(0, addressName, 2, 0, false);

      waitForBindings(1, addressName, 1, 0, true);
      waitForBindings(1, addressName, 2, 0, false);

      waitForBindings(2, addressName, 1, 0, true);
      waitForBindings(2, addressName, 2, 0, false);

      // send messages to node 0
      send(0, addressName, TEST_SIZE, true, null);

      // consume a message from queue 1 on node0
      addConsumer(1, 0, queueName1, null, false);

      ClientMessage clientMessage = consumers[1].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();
      consumers[1].getSession().commit();

      Assert.assertEquals(TEST_SIZE - 1, getMessageCount(((LocalQueueBinding) servers[0].getPostOffice().getBinding(new SimpleString(queueName1))).getQueue()));

      //check sf queue on server1 and 2 exists
      ClusterConnectionImpl clusterconn1 = (ClusterConnectionImpl) servers[1].getClusterManager().getClusterConnection("cluster0");
      SimpleString sfQueueNameTarget = clusterconn1.getSfQueueName(node0Id);
      QueueQueryResult result = servers[1].queueQuery(sfQueueNameTarget);
      assertTrue(result.isExists());

      clusterconn1 = (ClusterConnectionImpl) servers[2].getClusterManager().getClusterConnection("cluster0");
      SimpleString sfQueueNameOther = clusterconn1.getSfQueueName(node0Id);
      result = servers[2].queueQuery(sfQueueNameOther);
      assertTrue(result.isExists());

      consumers[1].getSession().close();
      // trigger scaleDown from node 0 to node 1
      servers[0].stop();

      waitForBindings(1, addressName, 1, 0, true);
      waitForBindings(1, addressName, 1, 0, false);

      waitForBindings(2, addressName, 1, 0, true);
      waitForBindings(2, addressName, 1, 0, false);

      //consumer from targetNode
      addConsumer(0, 1, queueName1, null);

      clientMessage = consumers[0].getConsumer().receive(5000000);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();

      // ensure there are no more messages on queue 1
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);

      //check
      checkNodeSfQueue(1, sfQueueNameTarget);

      checkNodeSfQueue(2, sfQueueNameOther);
   }

   private void checkNodeSfQueue(int node, SimpleString name) throws Exception {
      QueueQueryResult result = servers[node].queueQuery(name);
      AddressQueryResult result2 = servers[node].addressQuery(name);
      if ("true".equals(option)) {
         assertFalse("sf queue shouldn't exist on node " + node, result.isExists());
         assertFalse("sf address shouldn't exist on node " + node, result2.isExists());
      } else {
         assertTrue(result.isExists());
         assertTrue(result2.isExists());
      }
   }
}
