package by.palaznik.codecomplete.service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class FileService {

//    public boolean checkHash(String data, String hash) {
//        try {
//            MessageDigest md = MessageDigest.getInstance("MD5");
//            md.update(data.getBytes()); // Change this to "UTF-16" if needed
//            byte[] digest = md.digest();
//            byte[] hashBytes = hash.getBytes();
//            for (byte num : digest) {
//                System.out.print(num);
//            }
//            System.out.println();
//            for (byte num : hashBytes) {
//                System.out.print(num);
//            }
//            System.out.println(digest);
//            System.out.println(hashBytes);
//            if (Arrays.equals(digest, hashBytes)) {
//                return true;
//            }
//        } catch (NoSuchAlgorithmException e) {
//            e.printStackTrace();
//        }
//
//        return false;
//    }
}
