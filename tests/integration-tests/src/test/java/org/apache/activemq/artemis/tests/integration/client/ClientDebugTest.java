package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.version.Version;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.utils.VersionLoader;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.HashMap;

public class ClientDebugTest {

   @Test
   public void testDeployQueueVerify() throws Exception {
      deployQueueToBroker("InQueue");
   }

   private static void deployQueueToBroker(String queueName) throws Exception {
      Session session = null;
      Connection connection = null;
      Version clientVersion = VersionLoader.getVersion();
      System.out.println("Client version: " + clientVersion.getFullVersion());
      try {
         HashMap<String, Object> map = new HashMap<String, Object>();
         map.put("java.naming.factory.initial", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
         map.put("java.naming.provider.url", "tcp://127.0.0.1:61616");
         TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName(), map);
         ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transportConfiguration);
         Queue queue = ActiveMQJMSClient.createQueue(queueName);
         connection = cf.createConnection();
         session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(queue);
         producer.send(session.createTextMessage("initial message"));
         session.commit();
         connection.start();
         MessageConsumer consumer = session.createConsumer(queue);
         Message m = consumer.receive(1000);
         while (m != null) {
            System.out.println("got m: " + m);
            session.commit();
            System.out.println("message removed");
            m = consumer.receive(1000);
         }

      } catch (Exception e) {
         System.out.println("Got error deplying queue " + e);
         e.printStackTrace();
         throw new RuntimeException(e);
      } finally {
         if (connection != null) {
            connection.close();
         }
         if (session != null) {
            session.close();
         }
      }
   }

}
