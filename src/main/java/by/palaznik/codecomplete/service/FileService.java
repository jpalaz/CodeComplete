package by.palaznik.codecomplete.service;

import org.apache.commons.codec.digest.DigestUtils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class FileService {

    public static boolean checkHash(String dataBase64, String hash) {
        String data = new String(Base64.getDecoder().decode(dataBase64), StandardCharsets.UTF_8);
        System.out.println(data);
        String dataHash = DigestUtils.md5Hex(data);
        return hash.equals(dataHash);
    }
}
