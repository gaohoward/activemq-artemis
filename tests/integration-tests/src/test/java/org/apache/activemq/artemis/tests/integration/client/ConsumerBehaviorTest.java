package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class ConsumerBehaviorTest extends ActiveMQTestBase {

   public static final SimpleString ADDRESS = new SimpleString("SimpleAddress");


   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      ActiveMQServer server = createServer(true, createDefaultConfig(false));
      server.start();

      locator = createFactory(false);
   }

   @Test
   public void testDeliveryLogicBroken() throws Exception
   {
      //1. create a queue.
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession();
      session.createQueue(ADDRESS, ADDRESS, null, true);
      //2. add 3 consumers
      ClientConsumer consumer1 = session.createConsumer(ADDRESS);
      ClientConsumer consumer2 = session.createConsumer(ADDRESS);
      ClientConsumer consumer3 = session.createConsumer(ADDRESS);
      //3. deliver one message
      session.start();
      ClientMessage msg = session.createMessage(true);
      ClientProducer producer = session.createProducer(ADDRESS);
      producer.send(msg);

      QueueImpl.closeConsumer3Latch.await();
      consumer3.close();

      QueueImpl.consumer3CloasedLatch.countDown();

      Thread.sleep(10000);
   }
}
