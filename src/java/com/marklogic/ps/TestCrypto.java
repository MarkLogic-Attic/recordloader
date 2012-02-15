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
