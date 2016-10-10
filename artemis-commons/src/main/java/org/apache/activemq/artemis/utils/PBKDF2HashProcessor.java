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

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

/**
 * Hash function using PBKDF2 algothrim
 */
public class PBKDF2HashProcessor implements HashProcessor {

   private static final String SEPERATOR = ":";
   private static final String BEGIN_HASH = "ENC(";
   private static final String END_HASH = ")";
   private String sceretKeyAlgorithm = "PBKDF2WithHmacSHA1";
   private String randomScheme = "SHA1PRNG";
   private int keyLength = 64 * 8;
   private int saltLength = 32;
   private int iterations = 1024;
   private SecretKeyFactory skf;

   public PBKDF2HashProcessor() throws NoSuchAlgorithmException {
      skf = SecretKeyFactory.getInstance(sceretKeyAlgorithm);
   }

   @Override
   public String hash(String plainText) throws NoSuchAlgorithmException, InvalidKeySpecException {
      char[] chars = plainText.toCharArray();
      byte[] salt = getSalt();

      StringBuilder builder = new StringBuilder();
      builder.append(BEGIN_HASH).append(iterations).append(SEPERATOR).append(ByteUtil.bytesToHex(salt)).append(SEPERATOR);

      PBEKeySpec spec = new PBEKeySpec(chars, salt, iterations, keyLength);

      byte[] hash = skf.generateSecret(spec).getEncoded();
      String hexValue = ByteUtil.bytesToHex(hash);
      builder.append(hexValue).append(END_HASH);

      return builder.toString();
   }

   public byte[] getSalt() throws NoSuchAlgorithmException {
      byte[] salt = new byte[this.saltLength];

      SecureRandom sr = SecureRandom.getInstance(this.randomScheme);
      sr.nextBytes(salt);
      return salt;
   }

   @Override
   //storedValue must take form of ENC(...)
   public boolean compare(char[] plainChars, String storedValue) {
      String storedHash = storedValue.substring(4, storedValue.length() - 2);
      String[] parts = storedHash.split(SEPERATOR);
      int originalIterations = Integer.parseInt(parts[0]);
      byte[] salt = ByteUtil.hexToBytes(parts[1]);
      byte[] originalHash = ByteUtil.hexToBytes(parts[2]);

      PBEKeySpec spec = new PBEKeySpec(plainChars, salt, originalIterations, originalHash.length * 8);
      byte[] newHash;

      try {
         newHash = skf.generateSecret(spec).getEncoded();
      } catch (InvalidKeySpecException e) {
         return false;
      }

      return Arrays.equals(newHash, originalHash);
   }
}
