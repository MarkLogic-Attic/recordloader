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

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import java.io.PrintWriter;
import java.security.SecureRandom;

import java.awt.Point;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.IvParameterSpec;

public class Crypto {
    Cipher eCipher;
    Cipher dCipher;
    int last;
    Point point;
    Random random;
    Set set;
    ObjectInputStream ois;
    
    public Crypto(String passPhrase) throws Exception {
        
        set = new HashSet();
        random = new Random();
        for (int i = 0; i < 10; i++) {
            point = new Point(random.nextInt(1000), random.nextInt(2000));
            set.add(point);
        }
        last = random.nextInt(5000);
        try {
            // Create Key
            //byte key[] = passPhrase.getBytes();
            ois = new ObjectInputStream(new FileInputStream("keyfile"));
            DESKeySpec desKeySpec = new DESKeySpec((byte[]) ois.readObject());
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
            SecretKey secretKey = keyFactory.generateSecret(desKeySpec);
            
            // Create Cipher
            eCipher = Cipher.getInstance("DES/CFB8/NoPadding");
            dCipher = Cipher.getInstance("DES/CFB8/NoPadding");
            
            // Create the ciphers
            eCipher.init(Cipher.ENCRYPT_MODE, secretKey);
            dCipher.init(Cipher.DECRYPT_MODE, secretKey , new IvParameterSpec((byte[]) ois.readObject()));
            
        } catch (javax.crypto.NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (java.security.NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (java.security.InvalidKeyException e) {
            e.printStackTrace();
        }
        
    }
    
    public static void encryptPassword(String password) throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance("DES");
        kg.init(new SecureRandom());
        SecretKey key = kg.generateKey();
        SecretKeyFactory skf = SecretKeyFactory.getInstance("DES");
        Class spec = Class.forName("javax.crypto.spec.DESKeySpec");
        DESKeySpec ks = (DESKeySpec) skf.getKeySpec(key, spec);
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("keyfile"));
        oos.writeObject(ks.getKey());
        
        Cipher c = Cipher.getInstance("DES/CFB8/NoPadding");
        c.init(Cipher.ENCRYPT_MODE, key);
        CipherOutputStream cos = new CipherOutputStream(new FileOutputStream("ciphertext"), c);
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(cos));
        pw.println(password);
        pw.close();
        oos.writeObject(c.getIV());
        oos.close();
    }
    
    public static String decryptPassword() throws Exception {
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream("keyfile"));
        DESKeySpec ks = new DESKeySpec((byte[]) ois.readObject());
        SecretKeyFactory skf = SecretKeyFactory.getInstance("DES");
        SecretKey key = skf.generateSecret(ks);
        
        Cipher c = Cipher.getInstance("DES/CFB8/NoPadding");
        c.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec((byte[]) ois.readObject()));
        CipherInputStream cis = new CipherInputStream(new FileInputStream("ciphertext"), c);
        BufferedReader br = new BufferedReader(new InputStreamReader(cis));
        String decrypted = br.readLine();
        return decrypted;
    }
    
    
    
    public void encrypt(String str) {
        try {
            // Create stream
            FileOutputStream fos = new FileOutputStream("recordloader.des");
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            CipherOutputStream cos = new CipherOutputStream(bos, eCipher);
            ObjectOutputStream oos = new ObjectOutputStream(cos);
            
            // Write objects
            oos.writeObject(set);
            oos.writeInt(last);
            oos.flush();
            oos.close(); 
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
    
    public String decrypt() {
        try {
            CipherInputStream cis = new CipherInputStream(new FileInputStream("recordloader.des"), dCipher);
            BufferedReader br = new BufferedReader(new InputStreamReader(cis));
            String sDecrypted = br.readLine();
            System.out.println("Decrypted: " + sDecrypted);
            return sDecrypted;
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }        
        return null;
    }
}
