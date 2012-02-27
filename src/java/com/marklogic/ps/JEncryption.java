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
 
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
 
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
 
public class JEncryption
{    
public static void main(String[] argv) {
 
try{
 
   KeyGenerator keygenerator = KeyGenerator.getInstance("DES");
   SecretKey myDesKey = keygenerator.generateKey();
 
   Cipher desCipher;
 
   // Create the cipher 
   desCipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
 
   // Initialize the cipher for encryption
   desCipher.init(Cipher.ENCRYPT_MODE, myDesKey);
 
   //sensitive information
   byte[] text = "No body can see me".getBytes();
 
   System.out.println("Text [Byte Format] : " + text);
   System.out.println("Text : " + new String(text));
 
   // Encrypt the text
   byte[] textEncrypted = desCipher.doFinal(text);
 
   System.out.println("Text Encryted : " + textEncrypted);
 
   // Initialize the same cipher for decryption
   desCipher.init(Cipher.DECRYPT_MODE, myDesKey);
 
   // Decrypt the text
   byte[] textDecrypted = desCipher.doFinal(textEncrypted);
 
   System.out.println("Text Decryted : " + new String(textDecrypted));
 
}catch(NoSuchAlgorithmException e){
e.printStackTrace();
}catch(NoSuchPaddingException e){
e.printStackTrace();
}catch(InvalidKeyException e){
e.printStackTrace();
}catch(IllegalBlockSizeException e){
e.printStackTrace();
}catch(BadPaddingException e){
e.printStackTrace();
} 
 
}
}
