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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;

public class AuditLogEntry {

   protected String user = "anonymous";
   protected long timestamp;
   protected String operation;
   protected Object targetResource;
   protected String remoteAddr;

   SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss,SSS");

   protected Object[] parameters;

   public AuditLogEntry() {
      formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
   }

   public String getUser() {
      return user;
   }

   public void setUser(String user) {
      this.user = user;
   }

   public long getTimestamp() {
      return timestamp;
   }

   public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
   }

   public String getFormattedTime() {
      return formatter.format(new Date(timestamp));
   }

   public void setAction(String operation) {
      this.operation = operation;
   }

   public void setParameters(Object[] parameters) {
      this.parameters = parameters;
   }

   @Override
   public String toString() {
      return "User " + user.trim() + " is " + operation + " on target resource: " + targetResource + " with parameters: " + Arrays.toString(parameters) + " at " + getFormattedTime();
   }

   public void setTargetResource(Object targetResource) {
      this.targetResource = targetResource;
   }
}
