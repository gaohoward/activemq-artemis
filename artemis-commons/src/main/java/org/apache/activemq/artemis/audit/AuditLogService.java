/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.audit;

import javax.security.auth.Subject;
import java.security.AccessController;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

public class AuditLogService {

   public static final String CREATE_ADDRESS = "CreateAddress";
   public static final String UPDATE_ADDRESS = "UpdateAddress";
   public static final String DELETE_ADDRESS = "DeleteAddress";
   public static final String CREATE_QUEUE = "CreateQueue";
   public static final String UPDATE_QUEUE = "UpdateQueue";
   public static final String DELETE_QUEUE = "DeleteQueue";
   public static final String SEND_MESSAGE = "SendMessage";
   public static final String MOVE_MESSAGE = "MoveMessage";
   public static final String DELETE_MESSAGE = "DeleteMessage";
   public static final String DELETE_MESSAGES = "DeleteMessages";
   public static final String BROWSE_QUEUE = "BrowseQueue";
   public static final String CHANGE_MESSAGE_PRIORITY = "ChangeMessagePriority";
   public static final String COUNT_DELIVERY_MESSAGES = "CountDeliveryMessages";
   public static final String COUNT_MESSAGES = "CountMessages";
   public static final String EXPIRE_MESSAGE = "ExpireMessage";
   public static final String GET_CONSUMER_COUNT = "GetConsumerCount";
   public static final String GET_DELIVERING_COUNT = "GetDeliveringCount";
   public static final String GET_DELIVERING_SIZE = "GetDeliveringSize";
   public static final String GET_DURABLE_DELIVERING_COUNT = "GetDurableDeliveringCount";
   public static final String GET_DURABLE_DELIVERING_SIZE = "GetDurableDeliveringSize";
   public static final String GET_MESSAGES_ADDED = "GetMessagesAdded";
   public static final String GET_MESSAGES_ACKED = "GetMessagesAcked";
   public static final String GET_MESSAGES_EXPIRED = "GetMessagesExpired";
   public static final String GET_MESSAGES_KILLED = "GetMessagesKilled";
   public static final String GET_ID = "GetID";
   public static final String GET_SCHEDULED_COUNT = "GetScheduledCount";
   public static final String GET_SCHEDULED_SIZE = "GetScheduledSize";
   public static final String GET_NAME = "GetName";
   public static final String GET_FILTER = "GetFilter";
   public static final String IS_DURABLE = "IsDurable";
   public static final String GET_USER = "GetUser";
   public static final String GET_ROUTING_TYPE = "GetRoutingType";
   public static final String IS_TEMPORARY = "IsTemporary";
   public static final String GET_MESSAGE_COUNT = "GetMessageCount";
   public static final String GET_PERSISTENT_SIZE = "GetPersistentSize";
   public static final String GET_DURABLE_MESSAGE_COUNT = "GetDurableMessageCount";
   public static final String GET_DURABLE_PERSIST_SIZE = "GetDurablePersistSize";
   public static final String GET_DURABLE_SCHEDULED_COUNT = "GetDurableScheduledCount";
   public static final String GET_DURABLE_SCHEDULED_SIZE = "GetDurableScheduledSize";
   public static final String GET_DLA = "GetDLA";
   public static final String GET_EXPIRY_ADDRESS = "GetExpiryAddress";
   public static final String GET_MAX_CONSUMERS = "GetMaxConsumers";
   public static final String IS_PURGE_ON_NO_CONSUMERS = "IsPurgeOnNoConsumers";
   public static final String IS_CONFIGURATION_MANAGED = "IsConfigurationManaged";
   public static final String IS_EXCLUSIVE = "IsExclusive";
   public static final String IS_LAST_VALUE = "IsLastValue";
   public static final String LIST_SCHEDULED_MESSAGES = "ListScheduledMessages";
   public static final String LIST_DELIVERING_MESSAGES = "ListDeliveringMessages";
   public static final String LIST_MESSAGES = "ListMessages";
   public static final String GET_FIRST_MESSAGE_AS_JSON = "GetFirstMessageAsJson";
   public static final String GET_FIRST_MESSAGE_TIMESTAMP = "GetFirstMessageTimestamp";
   public static final String GET_FIRST_MESSAGE_AGE = "GetFirstMessageAge";
   public static final String RETRY_MESSAGE = "RetryMessage";
   public static final String RETRY_MESSAGES = "RetryMessages";
   public static final String LIST_MESSAGE_COUNTER = "ListMessageCounter";
   public static final String RESET_MESSAGE_COUNTER = "ResetMessageCounter";
   public static final String LIST_MESSAGE_COUNTER_AS_HTML = "ListMessageCounterAsHTML";
   public static final String LIST_MESSAGE_COUNTER_HISTORY = "ListMessageCounterHistory";
   public static final String LIST_MESSAGE_COUNTER_HISTORY_AS_HTML = "ListMessageCounterHistoryAsHTML";
   public static final String PAUSE = "Pause";
   public static final String RESUME = "Resume";
   public static final String IS_PAUSED = "IsPaused";
   public static final String RESET_ALL_GROUPS = "ResetAllGroups";
   public static final String RESET_GROUP = "ResetGroup";
   public static final String GET_GROUP_COUNT = "GetGroupCount";
   public static final String LIST_GROUPS_AS_JSON = "ListGroupsAsJson";
   public static final String LIST_CONSUMERS_AS_JSON = "ListConsumersAsJson";
   public static final String FLUSH_EXECUTOR = "FlushExecutor";
   public static final String RESET_MESSAGES_ACKED = "ResetMessagesAcked";
   public static final String RESET_MESSAGES_EXPIRED = "ResetMessagesExpired";
   public static final String RESET_MESSAGES_KILLED = "ResetMessagesKilled";
   public static final String RESET_MESSAGES_ADDED = "ResetMessagesAdded";
   public static final String CREATE_CONNECTOR_SERVICE = "CreateConnectorService";
   public static final String DESTROY_CONNECTOR_SERVICE = "DestroyConnectorService";
   public static final String ADD_ADDRESS_SETTINGS = "AddAddressSettings";
   public static final String GET_CONNECTOR_SERVICES = "GetConnectorServices";
   public static final String CLOSE_CONNECTIONS_FOR_ADDRESS = "CloseConnectionsForAddress";
   public static final String CLOSE_CONNECTIONS_FOR_USER = "CloseConnectionsForUser";
   public static final String CLOSE_CONNECTION_WITH_ID = "CloseConnectionsWithID";
   public static final String CLOSE_CONSUMER_CONNECTIONS_FOR_ADDRESS = "CloseConsumerConnectionsForAddress";

   private static final Map<String, String> actionMap = new HashMap<>();

   public static DefaultAuditLog auditLog = new DefaultAuditLog();

   static {
      actionMap.put(CREATE_ADDRESS, "creating an address");
      actionMap.put(UPDATE_ADDRESS, "updating an address");
      actionMap.put(DELETE_ADDRESS, "deleting an address");
      actionMap.put(CREATE_QUEUE, "creating a queue");
      actionMap.put(UPDATE_QUEUE, "updating a queue");
      actionMap.put(DELETE_QUEUE, "deleting a queue");
      actionMap.put(SEND_MESSAGE, "sending a message");
      actionMap.put(MOVE_MESSAGE, "moving a message");
      actionMap.put(DELETE_MESSAGE, "deleting a message");
      actionMap.put(DELETE_MESSAGES, "deleting messages");
      actionMap.put(BROWSE_QUEUE, "browsing a queue");
      actionMap.put(CHANGE_MESSAGE_PRIORITY, "changing message's priority");
      actionMap.put(COUNT_DELIVERY_MESSAGES, "counting delivery messages");
      actionMap.put(COUNT_MESSAGES, "counting messages");
      actionMap.put(EXPIRE_MESSAGE, "expiring messages");
      actionMap.put(GET_CONSUMER_COUNT, "getting consumer count");
      actionMap.put(GET_DELIVERING_COUNT, "getting delivering count");
      actionMap.put(GET_DELIVERING_SIZE, "getting delivering size");
      actionMap.put(GET_DURABLE_DELIVERING_COUNT, "getting durable delivering count");
      actionMap.put(GET_DURABLE_DELIVERING_SIZE, "getting durable delivering size");
      actionMap.put(GET_MESSAGES_ADDED, "getting messages added");
      actionMap.put(GET_MESSAGES_ACKED, "getting messages acknowledged");
      actionMap.put(GET_MESSAGES_EXPIRED, "getting messages expired");
      actionMap.put(GET_MESSAGES_KILLED, "getting messages killed");
      actionMap.put(GET_ID, "getting ID");
      actionMap.put(GET_SCHEDULED_COUNT, "getting scheduled count");
      actionMap.put(GET_SCHEDULED_SIZE, "getting scheduled size");
      actionMap.put(GET_NAME, "getting name");
      actionMap.put(GET_FILTER, "getting filter");
      actionMap.put(IS_DURABLE, "getting durable property");
      actionMap.put(GET_USER, "getting user property");
      actionMap.put(GET_ROUTING_TYPE, "getting routing type property");
      actionMap.put(IS_TEMPORARY, "getting temporary property");
      actionMap.put(GET_MESSAGE_COUNT, "getting message count");
      actionMap.put(GET_PERSISTENT_SIZE, "getting persistent size");
      actionMap.put(GET_DURABLE_MESSAGE_COUNT, "getting durable message count");
      actionMap.put(GET_DURABLE_PERSIST_SIZE, "getting durable persist size");
      actionMap.put(GET_DURABLE_SCHEDULED_COUNT, "getting durable scheduled count");
      actionMap.put(GET_DURABLE_SCHEDULED_SIZE, "getting durable scheduled size");
      actionMap.put(GET_DLA, "getting DLQ");
      actionMap.put(GET_EXPIRY_ADDRESS, "getting expiry address");
      actionMap.put(GET_MAX_CONSUMERS, "getting max consumers");
      actionMap.put(IS_PURGE_ON_NO_CONSUMERS, "getting purge-on-consumers property");
      actionMap.put(IS_CONFIGURATION_MANAGED, "getting configuration-managed property");
      actionMap.put(IS_EXCLUSIVE, "getting exclusive property");
      actionMap.put(IS_LAST_VALUE, "getting last-value property");
      actionMap.put(LIST_SCHEDULED_MESSAGES, "listing scheduled messages");
      actionMap.put(LIST_DELIVERING_MESSAGES, "listing delivering messages");
      actionMap.put(LIST_MESSAGES, "listing messages");
      actionMap.put(GET_FIRST_MESSAGE_AS_JSON, "getting first message as json");
      actionMap.put(GET_FIRST_MESSAGE_TIMESTAMP, "getting first message's timestamp");
      actionMap.put(GET_FIRST_MESSAGE_AGE, "getting first message's age");
      actionMap.put(RETRY_MESSAGE, "retry sending message");
      actionMap.put(RETRY_MESSAGES, "retry sending messages");
      actionMap.put(LIST_MESSAGE_COUNTER, "listing message counter");
      actionMap.put(RESET_MESSAGE_COUNTER, "resetting message counter");
      actionMap.put(LIST_MESSAGE_COUNTER_AS_HTML, "listing message counter as HTML");
      actionMap.put(LIST_MESSAGE_COUNTER_HISTORY, "listing message counter history");
      actionMap.put(LIST_MESSAGE_COUNTER_HISTORY_AS_HTML, "listing message counter history as HTML");
      actionMap.put(PAUSE, "pausing");
      actionMap.put(RESUME, "resuming");
      actionMap.put(IS_PAUSED, "getting paused property");
      actionMap.put(RESET_ALL_GROUPS, "resetting all groups");
      actionMap.put(RESET_GROUP, "resetting group");
      actionMap.put(GET_GROUP_COUNT, "getting group count");
      actionMap.put(LIST_GROUPS_AS_JSON, "listing groups as json");
      actionMap.put(LIST_CONSUMERS_AS_JSON, "listing consumers as json");
      actionMap.put(FLUSH_EXECUTOR, "flushing executor");
      actionMap.put(RESET_MESSAGES_ACKED, "resetting acknowledged messages");
      actionMap.put(RESET_MESSAGES_EXPIRED, "resetting expired messages");
      actionMap.put(RESET_MESSAGES_KILLED, "resetting killed messages");
      actionMap.put(RESET_MESSAGES_ADDED, "resetting added messages");
      actionMap.put(CREATE_CONNECTOR_SERVICE, "creating connector service");
      actionMap.put(DESTROY_CONNECTOR_SERVICE, "destroying connector service");
      actionMap.put(ADD_ADDRESS_SETTINGS, "adding addressSettings");
      actionMap.put(GET_CONNECTOR_SERVICES, "getting connector services");
      actionMap.put(CLOSE_CONNECTIONS_FOR_ADDRESS, "closing connections for address");
      actionMap.put(CLOSE_CONNECTIONS_FOR_USER, "closing connections for user");
      actionMap.put(CLOSE_CONNECTION_WITH_ID, "closing a connection by ID");
      actionMap.put(CLOSE_CONSUMER_CONNECTIONS_FOR_ADDRESS, "closing consumer connections for address");
   }

   public static void log(AuditLogEntry entry) {
      auditLog.log(entry);
   }

   public static void auditStart(String operation, Object targetResource, Object... params) {
      AuditLogEntry entry = new AuditLogEntry();
      entry.setUser(getCaller());
      entry.setAction(getAction(operation));
      entry.setTargetResource(targetResource);
      entry.setParameters(params);
      entry.setTimestamp(System.currentTimeMillis());
      auditLog.log(entry);
   }

   private static String getAction(String operation) {
      String action = actionMap.get(operation);
      if (action == null) {
         return operation;
      }
      return action;
   }

   private static String getCaller() {
      Subject subject = Subject.getSubject(AccessController.getContext());
      String caller = "anonymous";
      if (subject != null) {
         caller = "";
         for (Principal principal : subject.getPrincipals()) {
            caller += principal.getName() + "|";
         }
      }
      return caller;
   }

   public static void auditOK(Object result) {
      if (result == null) {
         auditLog.log("Operation finishes successfully.");
      } else {
         auditLog.log("Operation finishes successfully with result: " + result.toString());
      }
   }

   public static void auditFailure(Throwable cause) {
      auditLog.log("Operation failed with cause: " + cause);
   }

   public static <T> T audit(String op, Object targetResource, Auditable<T> method, Object... args) throws Exception {
      //don't audit anything outside the map
      if (getAction(op) == null) return method.operation();
      T result;
      try {
         AuditLogService.auditStart(op, targetResource, args);
         result = method.operation();
         if (result != null) {
            AuditLogService.auditOK(result);
         } else {
            AuditLogService.auditOK(null);
         }
         return result;
      } catch (Exception e) {
         AuditLogService.auditFailure(e);
         throw e;
      }
   }

   //for methods that doesn't throw exceptions
   public static <T> T audit2(String op, Object targetResource, Auditable<T> method, Object... args) {
      //don't audit anything outside the map
      if (getAction(op) == null) {
         try {
            return method.operation();
         } catch (Exception e) {
            throw new IllegalStateException("Unexpected exception", e);
         }
      }
      T result;
      try {
         AuditLogService.auditStart(op, targetResource, args);
         result = method.operation();
         if (result != null) {
            AuditLogService.auditOK(result);
         } else {
            AuditLogService.auditOK(null);
         }
         return result;
      } catch (Exception e) {
         AuditLogService.auditFailure(e);
         throw new IllegalStateException("Unexpected exception", e);
      }
   }
}
