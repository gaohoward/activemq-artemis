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
package org.apache.activemq.artemis.core.management.impl;

import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import javax.management.openmbean.CompositeData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.management.impl.openmbean.OpenTypeSupport;
import org.apache.activemq.artemis.core.messagecounter.MessageCounter;
import org.apache.activemq.artemis.core.messagecounter.impl.MessageCounterHelper;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.JsonLoader;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;

import static org.apache.activemq.artemis.audit.AuditLogService.BROWSE_QUEUE;
import static org.apache.activemq.artemis.audit.AuditLogService.CHANGE_MESSAGE_PRIORITY;
import static org.apache.activemq.artemis.audit.AuditLogService.COUNT_DELIVERY_MESSAGES;
import static org.apache.activemq.artemis.audit.AuditLogService.COUNT_MESSAGES;
import static org.apache.activemq.artemis.audit.AuditLogService.DELETE_MESSAGE;
import static org.apache.activemq.artemis.audit.AuditLogService.DELETE_MESSAGES;
import static org.apache.activemq.artemis.audit.AuditLogService.EXPIRE_MESSAGE;
import static org.apache.activemq.artemis.audit.AuditLogService.FLUSH_EXECUTOR;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_CONSUMER_COUNT;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_DELIVERING_COUNT;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_DELIVERING_SIZE;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_DLA;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_DURABLE_DELIVERING_COUNT;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_DURABLE_DELIVERING_SIZE;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_DURABLE_MESSAGE_COUNT;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_DURABLE_PERSIST_SIZE;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_DURABLE_SCHEDULED_COUNT;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_DURABLE_SCHEDULED_SIZE;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_EXPIRY_ADDRESS;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_FILTER;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_FIRST_MESSAGE_AGE;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_FIRST_MESSAGE_AS_JSON;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_FIRST_MESSAGE_TIMESTAMP;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_GROUP_COUNT;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_ID;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_MAX_CONSUMERS;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_MESSAGES_ACKED;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_MESSAGES_ADDED;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_MESSAGES_EXPIRED;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_MESSAGES_KILLED;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_MESSAGE_COUNT;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_NAME;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_PERSISTENT_SIZE;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_ROUTING_TYPE;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_SCHEDULED_COUNT;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_SCHEDULED_SIZE;
import static org.apache.activemq.artemis.audit.AuditLogService.GET_USER;
import static org.apache.activemq.artemis.audit.AuditLogService.IS_CONFIGURATION_MANAGED;
import static org.apache.activemq.artemis.audit.AuditLogService.IS_DURABLE;
import static org.apache.activemq.artemis.audit.AuditLogService.IS_EXCLUSIVE;
import static org.apache.activemq.artemis.audit.AuditLogService.IS_LAST_VALUE;
import static org.apache.activemq.artemis.audit.AuditLogService.IS_PAUSED;
import static org.apache.activemq.artemis.audit.AuditLogService.IS_PURGE_ON_NO_CONSUMERS;
import static org.apache.activemq.artemis.audit.AuditLogService.IS_TEMPORARY;
import static org.apache.activemq.artemis.audit.AuditLogService.LIST_CONSUMERS_AS_JSON;
import static org.apache.activemq.artemis.audit.AuditLogService.LIST_DELIVERING_MESSAGES;
import static org.apache.activemq.artemis.audit.AuditLogService.LIST_GROUPS_AS_JSON;
import static org.apache.activemq.artemis.audit.AuditLogService.LIST_MESSAGES;
import static org.apache.activemq.artemis.audit.AuditLogService.LIST_MESSAGE_COUNTER;
import static org.apache.activemq.artemis.audit.AuditLogService.LIST_MESSAGE_COUNTER_AS_HTML;
import static org.apache.activemq.artemis.audit.AuditLogService.LIST_MESSAGE_COUNTER_HISTORY;
import static org.apache.activemq.artemis.audit.AuditLogService.LIST_MESSAGE_COUNTER_HISTORY_AS_HTML;
import static org.apache.activemq.artemis.audit.AuditLogService.LIST_SCHEDULED_MESSAGES;
import static org.apache.activemq.artemis.audit.AuditLogService.MOVE_MESSAGE;
import static org.apache.activemq.artemis.audit.AuditLogService.PAUSE;
import static org.apache.activemq.artemis.audit.AuditLogService.RESET_ALL_GROUPS;
import static org.apache.activemq.artemis.audit.AuditLogService.RESET_GROUP;
import static org.apache.activemq.artemis.audit.AuditLogService.RESET_MESSAGES_ACKED;
import static org.apache.activemq.artemis.audit.AuditLogService.RESET_MESSAGES_ADDED;
import static org.apache.activemq.artemis.audit.AuditLogService.RESET_MESSAGES_EXPIRED;
import static org.apache.activemq.artemis.audit.AuditLogService.RESET_MESSAGES_KILLED;
import static org.apache.activemq.artemis.audit.AuditLogService.RESET_MESSAGE_COUNTER;
import static org.apache.activemq.artemis.audit.AuditLogService.RESUME;
import static org.apache.activemq.artemis.audit.AuditLogService.RETRY_MESSAGE;
import static org.apache.activemq.artemis.audit.AuditLogService.RETRY_MESSAGES;
import static org.apache.activemq.artemis.audit.AuditLogService.SEND_MESSAGE;
import static org.apache.activemq.artemis.audit.AuditLogService.audit;
import static org.apache.activemq.artemis.audit.AuditLogService.audit2;


public class QueueControlImpl extends AbstractControl implements QueueControl {

   public static final int FLUSH_LIMIT = 500;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Queue queue;

   private final String address;

   private final ActiveMQServer server;

   private final StorageManager storageManager;
   private final SecurityStore securityStore;
   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private MessageCounter counter;

   // Static --------------------------------------------------------

   private static String toJSON(final Map<String, Object>[] messages) {
      JsonArray array = toJSONMsgArray(messages);
      return array.toString();
   }

   private static JsonArray toJSONMsgArray(final Map<String, Object>[] messages) {
      JsonArrayBuilder array = JsonLoader.createArrayBuilder();
      for (Map<String, Object> message : messages) {
         array.add(JsonUtil.toJsonObject(message));
      }
      return array.build();
   }

   private static String toJSON(final Map<String, Map<String, Object>[]> messages) {
      JsonArrayBuilder arrayReturn = JsonLoader.createArrayBuilder();
      for (Map.Entry<String, Map<String, Object>[]> entry : messages.entrySet()) {
         JsonObjectBuilder objectItem = JsonLoader.createObjectBuilder();
         objectItem.add("consumerName", entry.getKey());
         objectItem.add("elements", toJSONMsgArray(entry.getValue()));
         arrayReturn.add(objectItem);
      }

      return arrayReturn.build().toString();
   }

   // Constructors --------------------------------------------------

   public QueueControlImpl(final Queue queue,
                           final String address,
                           final ActiveMQServer server,
                           final StorageManager storageManager,
                           final SecurityStore securityStore,
                           final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception {
      super(QueueControl.class, storageManager);
      this.queue = queue;
      this.address = address;
      this.server = server;
      this.storageManager = storageManager;
      this.securityStore = securityStore;
      this.addressSettingsRepository = addressSettingsRepository;
   }

   // Public --------------------------------------------------------

   public void setMessageCounter(final MessageCounter counter) {
      this.counter = counter;
   }

   // QueueControlMBean implementation ------------------------------

   @Override
   public String getName() {
      return audit2(GET_NAME, this.queue,
         ()->this.getNameInternal());
   }

   private String getNameInternal() {
      clearIO();
      try {
         return queue.getName().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getAddress() {
      checkStarted();

      return address;
   }

   @Override
   public String getFilter() {
      return audit2(GET_FILTER, this.queue,
         ()->this.getFilterInternal());
   }

   private String getFilterInternal() {
      checkStarted();

      clearIO();
      try {
         Filter filter = queue.getFilter();

         return filter != null ? filter.getFilterString().toString() : null;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isDurable() {
      return audit2(IS_DURABLE, this.queue,
         ()->this.isDurableInternal());
   }

   private boolean isDurableInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.isDurable();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getUser() {
      return audit2(GET_USER, this.queue,
         ()->this.getUserInternal());
   }

   private String getUserInternal() {
      checkStarted();

      clearIO();
      try {
         SimpleString user = queue.getUser();
         return user == null ? null : user.toString();
      } finally {
         blockOnIO();
      }
   }


   @Override
   public String getRoutingType() {
      return audit2(GET_ROUTING_TYPE, this.queue,
         ()->this.getRoutingTypeInternal());
   }

   private String getRoutingTypeInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getRoutingType().toString();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public boolean isTemporary() {
      return audit2(IS_TEMPORARY, this.queue,
         ()->this.isTemporaryInternal());
   }

   private boolean isTemporaryInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.isTemporary();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessageCount() {
      return audit2(GET_MESSAGE_COUNT, this.queue,
         ()->this.getMessageCountInternal());
   }

   private long getMessageCountInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessageCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getPersistentSize() {
      return audit2(GET_PERSISTENT_SIZE, this.queue,
         ()->this.getPersistentSizeInternal());
   }

   private long getPersistentSizeInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getPersistentSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDurableMessageCount() {
      return audit2(GET_DURABLE_MESSAGE_COUNT, this.queue,
         ()->this.getDurableMessageCountInternal());
   }

   private long getDurableMessageCountInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getDurableMessageCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDurablePersistentSize() {
      return audit2(GET_DURABLE_PERSIST_SIZE, this.queue,
         ()->this.getDurablePersistentSizeInternal());
   }

   private long getDurablePersistentSizeInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getDurablePersistentSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getConsumerCount() {
      return audit2(GET_CONSUMER_COUNT, this.queue,
         ()->this.getConsumerCountInternal());
   }

   private int getConsumerCountInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getConsumerCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getDeliveringCount() {
      return audit2(GET_DELIVERING_COUNT, this.queue,
         ()->this.getDeliveringCountInternal());
   }

   private int getDeliveringCountInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getDeliveringCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDeliveringSize() {
      return audit2(GET_DELIVERING_SIZE, this.queue,
         ()->this.getDeliveringSizeInternal());
   }

   private long getDeliveringSizeInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getDeliveringSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getDurableDeliveringCount() {
      return audit2(GET_DURABLE_DELIVERING_COUNT, this.queue,
         ()->this.getDurableDeliveringCountInternal());
   }

   private int getDurableDeliveringCountInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getDurableDeliveringCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDurableDeliveringSize() {
      return audit2(GET_DURABLE_DELIVERING_SIZE, this.queue,
         ()->this.getDurableDeliveringSizeInternal());
   }

   private long getDurableDeliveringSizeInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getDurableDeliveringSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessagesAdded() {
      return audit2(GET_MESSAGES_ADDED, this.queue,
         ()->this.getMessagesAddedInternal());
   }

   private long getMessagesAddedInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessagesAdded();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessagesAcknowledged() {
      return audit2(GET_MESSAGES_ACKED, this.queue,
         ()->this.getMessagesAcknowledgedInternal());
   }

   private long getMessagesAcknowledgedInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessagesAcknowledged();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessagesExpired() {
      return audit2(GET_MESSAGES_EXPIRED, this.queue,
         ()->this.getMessagesExpiredInternal());
   }

   private long getMessagesExpiredInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessagesExpired();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getMessagesKilled() {
      return audit2(GET_MESSAGES_KILLED, this.queue,
         ()->this.getMessagesKilledInternal());
   }

   private long getMessagesKilledInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getMessagesKilled();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getID() {
      return audit2(GET_ID, this.queue,
         ()->this.getIDInternal());
   }

   private long getIDInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getID();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getScheduledCount() {
      return audit2(GET_SCHEDULED_COUNT, this.queue,
         ()->this.getScheduledCountInternal());
   }

   private long getScheduledCountInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getScheduledCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getScheduledSize() {
      return audit2(GET_SCHEDULED_SIZE, this.queue,
         ()->this.getScheduledSizeInternal());
   }

   private long getScheduledSizeInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getScheduledSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDurableScheduledCount() {
      return audit2(GET_DURABLE_SCHEDULED_COUNT, this.queue,
         ()->this.getDurableScheduledCountInternal());
   }

   private long getDurableScheduledCountInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getDurableScheduledCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long getDurableScheduledSize() {
      return audit2(GET_DURABLE_SCHEDULED_SIZE, this.queue,
         ()->this.getDurableScheduledSizeInternal());
   }

   private long getDurableScheduledSizeInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getDurableScheduledSize();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getDeadLetterAddress() {
      return audit2(GET_DLA, this.queue,
         ()->this.getDeadLetterAddressInternal());
   }

   private String getDeadLetterAddressInternal() {
      checkStarted();

      clearIO();
      try {
         AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

         if (addressSettings != null && addressSettings.getDeadLetterAddress() != null) {
            return addressSettings.getDeadLetterAddress().toString();
         }
         return null;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getExpiryAddress() {
      return audit2(GET_EXPIRY_ADDRESS, this.queue,
         ()->this.getExpiryAddressInternal());
   }

   private String getExpiryAddressInternal() {
      checkStarted();

      clearIO();
      try {
         AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

         if (addressSettings != null && addressSettings.getExpiryAddress() != null) {
            return addressSettings.getExpiryAddress().toString();
         } else {
            return null;
         }
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getMaxConsumers() {
      return audit2(GET_MAX_CONSUMERS, this.queue,
         ()->this.getMaxConsumersInternal());
   }

   private int getMaxConsumersInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getMaxConsumers();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isPurgeOnNoConsumers() {
      return audit2(IS_PURGE_ON_NO_CONSUMERS, this.queue,
         ()->this.isPurgeOnNoConsumersInternal());
   }

   private boolean isPurgeOnNoConsumersInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.isPurgeOnNoConsumers();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isConfigurationManaged() {
      return audit2(IS_CONFIGURATION_MANAGED, this.queue,
         ()->this.isConfigurationManagedInternal());
   }

   private boolean isConfigurationManagedInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.isConfigurationManaged();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isExclusive() {
      return audit2(IS_EXCLUSIVE, this.queue,
         ()->this.isExclusiveInternal());
   }

   private boolean isExclusiveInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.isExclusive();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isLastValue() {
      return audit2(IS_LAST_VALUE, this.queue,
         ()->this.isLastValueInternal());
   }

   private boolean isLastValueInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.isLastValue();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Map<String, Object>[] listScheduledMessages() throws Exception {
      return audit(LIST_SCHEDULED_MESSAGES, this.queue,
         ()->this.listScheduledMessagesInternal());
   }

   private Map<String, Object>[] listScheduledMessagesInternal() throws Exception {
      checkStarted();

      clearIO();
      try {
         List<MessageReference> refs = queue.getScheduledMessages();
         return convertMessagesToMaps(refs);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listScheduledMessagesAsJSON() throws Exception {
      checkStarted();

      clearIO();
      try {
         return QueueControlImpl.toJSON(listScheduledMessages());
      } finally {
         blockOnIO();
      }
   }

   /**
    * @param refs
    * @return
    */
   private Map<String, Object>[] convertMessagesToMaps(List<MessageReference> refs) throws ActiveMQException {
      Map<String, Object>[] messages = new Map[refs.size()];
      int i = 0;
      for (MessageReference ref : refs) {
         Message message = ref.getMessage();
         messages[i++] = message.toMap();
      }
      return messages;
   }

   @Override
   public Map<String, Map<String, Object>[]> listDeliveringMessages() throws ActiveMQException {
      try {
         return audit(LIST_DELIVERING_MESSAGES, this.queue,
            ()->this.listDeliveringMessagesInternal());
      } catch (ActiveMQException ae) {
         throw ae;
      } catch (Exception e) {
         throw new RuntimeException("Unexpected exception", e);
      }
   }

   private Map<String, Map<String, Object>[]> listDeliveringMessagesInternal() throws ActiveMQException {
      checkStarted();

      clearIO();
      try {
         Map<String, List<MessageReference>> msgs = queue.getDeliveringMessages();

         Map<String, Map<String, Object>[]> msgRet = new HashMap<>();

         for (Map.Entry<String, List<MessageReference>> entry : msgs.entrySet()) {
            msgRet.put(entry.getKey(), convertMessagesToMaps(entry.getValue()));
         }
         return msgRet;
      } finally {
         blockOnIO();
      }

   }

   @Override
   public String listDeliveringMessagesAsJSON() throws Exception {
      checkStarted();

      clearIO();
      try {
         return QueueControlImpl.toJSON(listDeliveringMessages());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Map<String, Object>[] listMessages(final String filterStr) throws Exception {
      return audit(LIST_MESSAGES, this.queue,
         ()->this.listMessagesInternal(filterStr), filterStr);
   }

   private Map<String, Object>[] listMessagesInternal(final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         List<Map<String, Object>> messages = new ArrayList<>();
         queue.flushExecutor();
         try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
            try {
               while (iterator.hasNext()) {
                  MessageReference ref = iterator.next();
                  if (filter == null || filter.match(ref.getMessage())) {
                     Message message = ref.getMessage();
                     messages.add(message.toMap());
                  }
               }
            } catch (NoSuchElementException ignored) {
               // this could happen through paging browsing
            }
            return messages.toArray(new Map[messages.size()]);
         }
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listMessagesAsJSON(final String filter) throws Exception {
      checkStarted();

      clearIO();
      try {
         return QueueControlImpl.toJSON(listMessages(filter));
      } finally {
         blockOnIO();
      }
   }

   protected Map<String, Object>[] getFirstMessage() throws Exception {
      checkStarted();

      clearIO();
      try {
         List<Map<String, Object>> messages = new ArrayList<>();
         queue.flushExecutor();
         try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
            // returns just the first, as it's the first only
            if (iterator.hasNext()) {
               MessageReference ref = iterator.next();
               Message message = ref.getMessage();
               messages.add(message.toMap());
            }
            return messages.toArray(new Map[1]);
         }
      } finally {
         blockOnIO();
      }

   }

   @Override
   public String getFirstMessageAsJSON() throws Exception {
      return audit(GET_FIRST_MESSAGE_AS_JSON, this.queue,
         ()->this.getFirstMessageAsJSONInternal());
   }

   private String getFirstMessageAsJSONInternal() throws Exception {
      return toJSON(getFirstMessage());
   }

   @Override
   public Long getFirstMessageTimestamp() throws Exception {
      return audit(GET_FIRST_MESSAGE_TIMESTAMP, this.queue,
         ()->this.getFirstMessageTimestampInternal());
   }

   private Long getFirstMessageTimestampInternal() throws Exception {
      Map<String, Object>[] _message = getFirstMessage();
      if (_message == null || _message.length == 0 || _message[0] == null) {
         return null;
      }
      Map<String, Object> message = _message[0];
      if (!message.containsKey("timestamp")) {
         return null;
      }
      return (Long) message.get("timestamp");
   }

   @Override
   public Long getFirstMessageAge() throws Exception {
      return audit(GET_FIRST_MESSAGE_AGE, this.queue,
         ()->this.getFirstMessageAgeInternal());
   }

   private Long getFirstMessageAgeInternal() throws Exception {
      Long firstMessageTimestamp = getFirstMessageTimestamp();
      if (firstMessageTimestamp == null) {
         return null;
      }
      long now = new Date().getTime();
      return now - firstMessageTimestamp.longValue();
   }

   @Override
   public long countMessages() throws Exception {
      return countMessages(null);
   }

   @Override
   public long countMessages(final String filterStr) throws Exception {
      Long value = intenalCountMessages(filterStr, null).get(null);
      return value == null ? 0 : value;
   }

   @Override
   public String countMessages(final String filterStr, final String groupByProperty) throws Exception {
      return JsonUtil.toJsonObject(intenalCountMessages(filterStr, groupByProperty)).toString();
   }

   private Map<String, Long> intenalCountMessages(final String filterStr, final String groupByPropertyStr) throws Exception {
      return audit(COUNT_MESSAGES, this.queue,
         ()->this.internalCountMessagesInternal(filterStr, groupByPropertyStr),
               filterStr, groupByPropertyStr);
   }

   private Map<String, Long> internalCountMessagesInternal(final String filterStr, final String groupByPropertyStr) throws Exception {
      checkStarted();

      clearIO();

      Map<String, Long> result = new HashMap<>();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         SimpleString groupByProperty = SimpleString.toSimpleString(groupByPropertyStr);
         if (filter == null && groupByProperty == null) {
            result.put(null, getMessageCount());
         } else {
            try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
               try {
                  while (iterator.hasNext()) {
                     Message message = iterator.next().getMessage();
                     internalComputeMessage(result, filter, groupByProperty, message);
                  }
               } catch (NoSuchElementException ignored) {
                  // this could happen through paging browsing
               }
            }
         }
         return result;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public long countDeliveringMessages(final String filterStr) throws Exception {
      Long value = intenalCountDeliveryMessages(filterStr, null).get(null);
      return value == null ? 0 : value;
   }

   @Override
   public String countDeliveringMessages(final String filterStr, final String groupByProperty) throws Exception {
      return JsonUtil.toJsonObject(intenalCountDeliveryMessages(filterStr, groupByProperty)).toString();
   }

   private Map<String, Long> intenalCountDeliveryMessages(final String filterStr, final String groupByPropertyStr) throws Exception {
      return audit(COUNT_DELIVERY_MESSAGES, this.queue,
         ()->this.intenalCountDeliveryMessagesInternal(filterStr, groupByPropertyStr), filterStr, groupByPropertyStr);
   }

   private Map<String, Long> intenalCountDeliveryMessagesInternal(final String filterStr, final String groupByPropertyStr) throws Exception {
      checkStarted();

      clearIO();

      Map<String, Long> result = new HashMap<>();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         SimpleString groupByProperty = SimpleString.toSimpleString(groupByPropertyStr);
         if (filter == null && groupByProperty == null) {
            result.put(null, Long.valueOf(getDeliveringCount()));
         } else {
            Map<String, List<MessageReference>> deliveringMessages = queue.getDeliveringMessages();
            deliveringMessages.forEach((s, messageReferenceList) ->
                            messageReferenceList.forEach(messageReference ->
                                    internalComputeMessage(result, filter, groupByProperty, messageReference.getMessage())
                            ));
         }
         return result;
      } finally {
         blockOnIO();
      }
   }

   private void internalComputeMessage(Map<String, Long> result, Filter filter, SimpleString groupByProperty, Message message) {
      if (filter == null || filter.match(message)) {
         if (groupByProperty == null) {
            result.compute(null, (k, v) -> v == null ? 1 : ++v);
         } else {
            Object value = message.getObjectProperty(groupByProperty);
            String valueStr = value == null ? null : value.toString();
            result.compute(valueStr, (k, v) -> v == null ? 1 : ++v);
         }
      }
   }


   @Override
   public boolean removeMessage(final long messageID) throws Exception {
      return audit(DELETE_MESSAGE, this.queue,
         ()->this.removeMessageInternal(messageID), messageID);
   }

   private boolean removeMessageInternal(final long messageID) throws Exception {
      checkStarted();

      clearIO();
      try {
         return queue.deleteReference(messageID);
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int removeMessages(final String filterStr) throws Exception {
      return removeMessages(FLUSH_LIMIT, filterStr);
   }

   @Override
   public int removeMessages(final int flushLimit, final String filterStr) throws Exception {
      return audit(DELETE_MESSAGES, this.queue,
         ()->this.removeMessagesInternal(flushLimit, filterStr),
               flushLimit, filterStr);
   }

   private int removeMessagesInternal(final int flushLimit, final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);

         return queue.deleteMatchingReferences(flushLimit, filter);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int removeAllMessages() throws Exception {
      return removeMessages(FLUSH_LIMIT, null);
   }

   @Override
   public boolean expireMessage(final long messageID) throws Exception {
      return audit(EXPIRE_MESSAGE, this.queue,
         ()->this.expireMessageInternal(messageID), messageID);
   }

   private boolean expireMessageInternal(final long messageID) throws Exception {
      checkStarted();

      clearIO();
      try {
         return queue.expireReference(messageID);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int expireMessages(final String filterStr) throws Exception {
      return audit(EXPIRE_MESSAGE, this.queue,
         ()->this.expireMessagesInternal(filterStr), filterStr);
   }

   private int expireMessagesInternal(final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);
         return queue.expireReferences(filter);
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean retryMessage(final long messageID) throws Exception {
      return audit(RETRY_MESSAGE, this.queue,
         ()->this.retryMessageInternal(messageID), messageID);
   }

   private boolean retryMessageInternal(final long messageID) throws Exception {

      checkStarted();
      clearIO();

      try {
         Filter singleMessageFilter = new Filter() {
            @Override
            public boolean match(Message message) {
               return message.getMessageID() == messageID;
            }

            @Override
            public SimpleString getFilterString() {
               return new SimpleString("custom filter for MESSAGEID= messageID");
            }
         };

         return queue.retryMessages(singleMessageFilter) > 0;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int retryMessages() throws Exception {
      return audit(RETRY_MESSAGES, this.queue,
         ()->this.retryMessagesInternal());
   }

   private int retryMessagesInternal() throws Exception {
      checkStarted();
      clearIO();

      try {
         return queue.retryMessages(null);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean moveMessage(final long messageID, final String otherQueueName) throws Exception {
      return moveMessage(messageID, otherQueueName, false);
   }

   @Override
   public boolean moveMessage(final long messageID,
                              final String otherQueueName,
                              final boolean rejectDuplicates) throws Exception {
      return audit(MOVE_MESSAGE, this.queue,
         ()->this.moveMessageInternal(messageID, otherQueueName, rejectDuplicates),
               messageID, otherQueueName, rejectDuplicates);
   }

   private boolean moveMessageInternal(final long messageID,
                              final String otherQueueName,
                              final boolean rejectDuplicates) throws Exception {
      checkStarted();

      clearIO();
      try {
         Binding binding = server.getPostOffice().getBinding(new SimpleString(otherQueueName));

         if (binding == null) {
            throw ActiveMQMessageBundle.BUNDLE.noQueueFound(otherQueueName);
         }

         return queue.moveReference(messageID, binding.getAddress(), binding, rejectDuplicates);
      } finally {
         blockOnIO();
      }

   }

   @Override
   public int moveMessages(final String filterStr, final String otherQueueName) throws Exception {
      return moveMessages(filterStr, otherQueueName, false);
   }

   @Override
   public int moveMessages(final int flushLimit,
                           final String filterStr,
                           final String otherQueueName,
                           final boolean rejectDuplicates) throws Exception {
      return audit(MOVE_MESSAGE, this.queue,
         ()->this.moveMessagesInternal(flushLimit, filterStr, otherQueueName, rejectDuplicates),
               flushLimit, filterStr, otherQueueName, rejectDuplicates);
   }

   private int moveMessagesInternal(final int flushLimit,
                           final String filterStr,
                           final String otherQueueName,
                           final boolean rejectDuplicates) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);

         Binding binding = server.getPostOffice().getBinding(new SimpleString(otherQueueName));

         if (binding == null) {
            throw ActiveMQMessageBundle.BUNDLE.noQueueFound(otherQueueName);
         }

         int retValue = queue.moveReferences(flushLimit, filter, binding.getAddress(), rejectDuplicates, binding);

         return retValue;
      } finally {
         blockOnIO();
      }

   }

   @Override
   public int moveMessages(final String filterStr,
                           final String otherQueueName,
                           final boolean rejectDuplicates) throws Exception {
      return moveMessages(FLUSH_LIMIT, filterStr, otherQueueName, rejectDuplicates);
   }

   @Override
   public int sendMessagesToDeadLetterAddress(final String filterStr) throws Exception {
      return audit(SEND_MESSAGE, this.queue,
         ()->this.sendMessagesToDeadLetterAddressInternal(filterStr), filterStr);
   }

   private int sendMessagesToDeadLetterAddressInternal(final String filterStr) throws Exception {
      checkStarted();

      clearIO();
      try {
         Filter filter = FilterImpl.createFilter(filterStr);

         return queue.sendMessagesToDeadLetterAddress(filter);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String sendMessage(final Map<String, String> headers,
                             final int type,
                             final String body,
                             boolean durable,
                             final String user,
                             final String password) throws Exception {
      try {
         return sendMessage(queue.getAddress(), server, headers, type, body, durable, user, password, queue.getID());
      } catch (Exception e) {
         throw new IllegalStateException(e.getMessage());
      }
   }

   @Override
   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception {
      return audit(SEND_MESSAGE, this.queue,
         ()->this.sendMessageToDeadLetterAddressInternal(messageID),
               messageID);
   }

   private boolean sendMessageToDeadLetterAddressInternal(final long messageID) throws Exception {
      checkStarted();

      clearIO();
      try {
         return queue.sendMessageToDeadLetterAddress(messageID);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int changeMessagesPriority(final String filterStr, final int newPriority) throws Exception {
      return audit(CHANGE_MESSAGE_PRIORITY, this.queue,
         ()->this.changeMessagesPriorityInternal(filterStr, newPriority), filterStr, newPriority);
   }

   private int changeMessagesPriorityInternal(final String filterStr, final int newPriority) throws Exception {
      checkStarted();

      clearIO();
      try {
         if (newPriority < 0 || newPriority > 9) {
            throw ActiveMQMessageBundle.BUNDLE.invalidNewPriority(newPriority);
         }
         Filter filter = FilterImpl.createFilter(filterStr);

         return queue.changeReferencesPriority(filter, (byte) newPriority);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean changeMessagePriority(final long messageID, final int newPriority) throws Exception {
      return audit(CHANGE_MESSAGE_PRIORITY, this.queue,
         ()->this.changeMessagePriorityInternal(messageID, newPriority), messageID, newPriority);
   }

   private boolean changeMessagePriorityInternal(final long messageID, final int newPriority) throws Exception {
      checkStarted();

      clearIO();
      try {
         if (newPriority < 0 || newPriority > 9) {
            throw ActiveMQMessageBundle.BUNDLE.invalidNewPriority(newPriority);
         }
         return queue.changeReferencePriority(messageID, (byte) newPriority);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listMessageCounter() {
      return audit2(LIST_MESSAGE_COUNTER, this.queue,
         ()->this.listMessageCounterInternal());
   }

   private String listMessageCounterInternal() {
      checkStarted();

      clearIO();
      try {
         return counter.toJSon();
      } catch (Exception e) {
         throw new IllegalStateException(e);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void resetMessageCounter() {
      audit2(RESET_MESSAGE_COUNTER, this.queue,
         ()-> {
            this.resetMessageCounterInternal();
            return null;
         });
   }

   private void resetMessageCounterInternal() {
      checkStarted();

      clearIO();
      try {
         counter.resetCounter();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listMessageCounterAsHTML() {
      return audit2(LIST_MESSAGE_COUNTER_AS_HTML, this.queue,
         ()->this.listMessageCounterAsHTMLInternal());
   }

   private String listMessageCounterAsHTMLInternal() {
      checkStarted();

      clearIO();
      try {
         return MessageCounterHelper.listMessageCounterAsHTML(new MessageCounter[]{counter});
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listMessageCounterHistory() throws Exception {
      return audit(LIST_MESSAGE_COUNTER_HISTORY, this.queue,
         ()->this.listMessageCounterHistoryInternal());
   }

   private String listMessageCounterHistoryInternal() throws Exception {
      checkStarted();

      clearIO();
      try {
         return MessageCounterHelper.listMessageCounterHistory(counter);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listMessageCounterHistoryAsHTML() {
      return audit2(LIST_MESSAGE_COUNTER_HISTORY_AS_HTML, this.queue,
         ()->this.listMessageCounterHistoryAsHTMLInternal());
   }

   private String listMessageCounterHistoryAsHTMLInternal() {
      checkStarted();

      clearIO();
      try {
         return MessageCounterHelper.listMessageCounterHistoryAsHTML(new MessageCounter[]{counter});
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void pause() {
      audit2(PAUSE, this.queue,
         ()-> {
            this.pauseInternal();
            return null;
         });
   }

   private void pauseInternal() {
      checkStarted();

      clearIO();
      try {
         queue.pause();
      } finally {
         blockOnIO();
      }
   }


   @Override
   public void pause(boolean persist) {
      audit2(PAUSE, this.queue,
         ()-> {
            this.pauseInternal(persist);
            return null;
         }, persist);
   }

   private void pauseInternal(boolean persist) {
      checkStarted();

      clearIO();
      try {
         queue.pause(persist);
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void resume() {
      audit2(RESUME, this.queue,
         ()-> {
            this.resumeInternal();
            return null;
         });
   }

   private void resumeInternal() {
      checkStarted();

      clearIO();
      try {
         queue.resume();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isPaused() throws Exception {
      return audit(IS_PAUSED, this.queue,
         ()->this.isPausedInternal());
   }

   private boolean isPausedInternal() throws Exception {
      checkStarted();

      clearIO();
      try {
         return queue.isPaused();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public CompositeData[] browse(int page, int pageSize) throws Exception {
      return audit(BROWSE_QUEUE, this.queue,
         ()->this.browseInternal(page, pageSize), page, pageSize);
   }

   private CompositeData[] browseInternal(int page, int pageSize) throws Exception {
      String filter = null;
      checkStarted();

      clearIO();
      try {
         long index = 0;
         long start = (long) (page - 1) * pageSize;
         long end = Math.min(page * pageSize, queue.getMessageCount());

         ArrayList<CompositeData> c = new ArrayList<>();
         Filter thefilter = FilterImpl.createFilter(filter);
         queue.flushExecutor();

         try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
            try {
               while (iterator.hasNext() && index < end) {
                  MessageReference ref = iterator.next();
                  if (thefilter == null || thefilter.match(ref.getMessage())) {
                     if (index >= start) {
                        c.add(OpenTypeSupport.convert(ref));
                     }
                  }
                  index++;
               }
            } catch (NoSuchElementException ignored) {
               // this could happen through paging browsing
            }

            CompositeData[] rc = new CompositeData[c.size()];
            c.toArray(rc);
            return rc;
         }
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public CompositeData[] browse() throws Exception {
      return browse(null);
   }

   @Override
   public CompositeData[] browse(String filter) throws Exception {
      return audit(BROWSE_QUEUE, this.queue,
         ()->this.browseInternal(filter), filter);
   }

   private CompositeData[] browseInternal(String filter) throws Exception {
      checkStarted();

      clearIO();
      try {
         int pageSize = addressSettingsRepository.getMatch(queue.getName().toString()).getManagementBrowsePageSize();
         int currentPageSize = 0;
         ArrayList<CompositeData> c = new ArrayList<>();
         Filter thefilter = FilterImpl.createFilter(filter);
         queue.flushExecutor();
         try (LinkedListIterator<MessageReference> iterator = queue.browserIterator()) {
            try {
               while (iterator.hasNext() && currentPageSize++ < pageSize) {
                  MessageReference ref = iterator.next();
                  if (thefilter == null || thefilter.match(ref.getMessage())) {
                     c.add(OpenTypeSupport.convert(ref));

                  }
               }
            } catch (NoSuchElementException ignored) {
               // this could happen through paging browsing
            }

            CompositeData[] rc = new CompositeData[c.size()];
            c.toArray(rc);
            return rc;
         }
      } catch (ActiveMQException e) {
         throw new IllegalStateException(e.getMessage());
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void flushExecutor() {
      audit2(FLUSH_EXECUTOR, this.queue,
         ()-> {
            this.flushExecutorInternal();
            return null;
         });
   }

   private void flushExecutorInternal() {
      checkStarted();

      clearIO();
      try {
         queue.flushExecutor();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void resetAllGroups() {
      audit2(RESET_ALL_GROUPS, this.queue,
         ()-> {
            this.resetAllGroupsInternal();
            return null;
         });
   }

   private void resetAllGroupsInternal() {
      checkStarted();

      clearIO();
      try {
         queue.resetAllGroups();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void resetGroup(String groupID) {
      audit2(RESET_GROUP, this.queue,
         ()-> {
            this.resetGroupInternal(groupID);
            return null;
         }, groupID);
   }

   private void resetGroupInternal(String groupID) {
      checkStarted();

      clearIO();
      try {
         queue.resetGroup(SimpleString.toSimpleString(groupID));
      } finally {
         blockOnIO();
      }
   }

   @Override
   public int getGroupCount() {
      return audit2(GET_GROUP_COUNT, this.queue,
         ()->this.getGroupCountInternal());
   }

   private int getGroupCountInternal() {
      checkStarted();

      clearIO();
      try {
         return queue.getGroupCount();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listGroupsAsJSON() throws Exception {
      return audit(LIST_GROUPS_AS_JSON, this.queue,
         ()->this.listGroupsAsJSONInternal());
   }

   private String listGroupsAsJSONInternal() throws Exception {
      checkStarted();

      clearIO();
      try {
         Map<SimpleString, Consumer> groups = queue.getGroups();

         JsonArrayBuilder jsonArray = JsonLoader.createArrayBuilder();

         for (Map.Entry<SimpleString, Consumer> group : groups.entrySet()) {

            if (group.getValue() instanceof ServerConsumer) {
               ServerConsumer serverConsumer = (ServerConsumer) group.getValue();

               JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("groupID", group.getKey().toString()).add("consumerID", serverConsumer.getID()).add("connectionID", serverConsumer.getConnectionID().toString()).add("sessionID", serverConsumer.getSessionID()).add("browseOnly", serverConsumer.isBrowseOnly()).add("creationTime", serverConsumer.getCreationTime());

               jsonArray.add(obj);
            }

         }

         return jsonArray.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String listConsumersAsJSON() throws Exception {
      return audit(LIST_CONSUMERS_AS_JSON, this.queue,
         ()->this.listConsumersAsJSONInternal());
   }

   private String listConsumersAsJSONInternal() throws Exception {
      checkStarted();

      clearIO();
      try {
         Collection<Consumer> consumers = queue.getConsumers();

         JsonArrayBuilder jsonArray = JsonLoader.createArrayBuilder();

         for (Consumer consumer : consumers) {

            if (consumer instanceof ServerConsumer) {
               ServerConsumer serverConsumer = (ServerConsumer) consumer;

               JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("consumerID", serverConsumer.getID()).add("connectionID", serverConsumer.getConnectionID().toString()).add("sessionID", serverConsumer.getSessionID()).add("browseOnly", serverConsumer.isBrowseOnly()).add("creationTime", serverConsumer.getCreationTime());

               jsonArray.add(obj);
            }

         }

         return jsonArray.build().toString();
      } finally {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(QueueControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(QueueControl.class);
   }

   @Override
   public void resetMessagesAdded() throws Exception {
      audit(RESET_MESSAGES_ADDED, this.queue,
         ()-> {
            this.resetMessagesAddedInternal();
            return null;
         });
   }

   private void resetMessagesAddedInternal() throws Exception {
      checkStarted();

      clearIO();
      try {
         queue.resetMessagesAdded();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public void resetMessagesAcknowledged() throws Exception {
      audit(RESET_MESSAGES_ACKED, this.queue,
         ()-> {
            this.resetMessagesAcknowledgedInternal();
            return null;
         });
   }

   private void resetMessagesAcknowledgedInternal() throws Exception {
      checkStarted();

      clearIO();
      try {
         queue.resetMessagesAcknowledged();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public void resetMessagesExpired() throws Exception {
      audit(RESET_MESSAGES_EXPIRED, this.queue,
         ()-> {
            this.resetMessagesExpiredInternal();
            return null;
         });
   }

   private void resetMessagesExpiredInternal() throws Exception {
      checkStarted();

      clearIO();
      try {
         queue.resetMessagesExpired();
      } finally {
         blockOnIO();
      }

   }

   @Override
   public void resetMessagesKilled() throws Exception {
      audit(RESET_MESSAGES_KILLED, this.queue,
         ()-> {
            this.resetMessagesKilledInternal();
            return null;
         });
   }

   private void resetMessagesKilledInternal() throws Exception {
      checkStarted();

      clearIO();
      try {
         queue.resetMessagesKilled();
      } finally {
         blockOnIO();
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void checkStarted() {
      if (!server.getPostOffice().isStarted()) {
         throw new IllegalStateException("Broker is not started. Queue can not be managed yet");
      }
   }

   // Inner classes -------------------------------------------------
}
