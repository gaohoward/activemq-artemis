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
package org.apache.activemq.artemis.utils;


/**
 * Used to process Hash text for passwords
 */
public interface HashProcessor {

   /**
    * produce hash text from plain text
    * @param plainText Plain text input
    * @return the Hash value of the input plain text
    * @throws Exception
    */
   String hash(String plainText) throws Exception;

   /**
    * compare the plain char array against the hash value
    * @param plainChars char array of the plain text
    * @param storedHash the existing hash value
    * @return true if the char array matches the hash value,
    * otherwise false.
    */
   boolean compare(char[] plainChars, String storedHash);
}
