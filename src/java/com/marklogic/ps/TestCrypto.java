/*
 * Copyright (c)2005-2012 Mark Logic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.ps;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

public class TestCrypto {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
		    // Generate a temporary key. In practice, you would save this key.
		    // See also Encrypting with DES Using a Pass Phrase.
		    SecretKey key = KeyGenerator.getInstance("DES").generateKey();

		    // Create encrypter/decrypter class
		    Crypto.encryptPassword("Don't tell anybody!");

		    // Decrypt
		    String decrypted = Crypto.decryptPassword();
		    System.out.println("******* decrypted: " + decrypted);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
