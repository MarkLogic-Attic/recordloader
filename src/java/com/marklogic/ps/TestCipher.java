package com.marklogic.ps;

import java.awt.Point;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.io.BufferedReader;
import java.io.InputStreamReader;


import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

public class TestCipher {

    // Password must be at least 8 characters
    private static final String password = "zukowski";

    public static void main(String args[]) throws Exception {
        Set set = new HashSet();
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            Point point = new Point(random.nextInt(1000), random.nextInt(2000));
            set.add(point);
        }
        int last = random.nextInt(5000);

        // Create Key
        byte key[] = password.getBytes();
        DESKeySpec desKeySpec = new DESKeySpec(key);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
        SecretKey secretKey = keyFactory.generateSecret(desKeySpec);

        // Create Cipher
        Cipher desCipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
        desCipher.init(Cipher.ENCRYPT_MODE, secretKey);

        // Create stream
        FileOutputStream fos = new FileOutputStream("recordloader.des");
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        CipherOutputStream cos = new CipherOutputStream(bos, desCipher);
        ObjectOutputStream oos = new ObjectOutputStream(cos);

        // Write objects
        oos.writeObject(set);
        oos.writeInt(last);
        oos.flush();
        oos.close();

        // Change cipher mode
        desCipher.init(Cipher.DECRYPT_MODE, secretKey);

        // Create stream
        FileInputStream fis = new FileInputStream("recordloader.des");
        BufferedInputStream bis = new BufferedInputStream(fis);
        CipherInputStream cis = new CipherInputStream(bis, desCipher);
        //ObjectInputStream ois = new ObjectInputStream(cis);

        BufferedReader br = new BufferedReader(new InputStreamReader(cis));
        System.out.println(br.readLine());

    }
}

