package com.marklogic.ps;

public class PasswordEncrypter {

	public static void main(String[] args) throws Exception {

		if (args.length == 1) {
		    String password = args[0];
		    Crypto.encryptPassword(password);

		    System.out.println("Encrypted password");
		} else {
			System.out.println("Please enter password to be encrypted.");
		}

	}

}

