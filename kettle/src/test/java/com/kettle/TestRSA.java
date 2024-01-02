package com.kettle;

import com.org.kettle.util.RsaUtils;
import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

public class TestRSA {

    @Test
    public void testpass() {
        BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
        String encode = passwordEncoder.encode("123456");
        System.out.println(encode);
    }

    @Test
    public void createKeyPair() throws Exception {
        //String privateFilePath = ResourceUtils.getFile("classpath:rsa").getPath();
        //String publicFilePath = ResourceUtils.getFile("classpath:rsa.pub").getPath();
        String privateFilePath = "D:\\rsa_key\\rsa.pri";
        String publicFilePath = "D:\\rsa_key\\rsa.pub";

        //生成密钥对的公共方法。这个方法需要传入4个参数
        /*
            参数1：生成公钥的存储位置
            参数2：生成私钥的存储位置
            参数3：生成密钥对的密钥
            参数3：生成密钥对的长度
         */
        RsaUtils.generateKey(publicFilePath, privateFilePath, "itsource", 2048);
        //获取私钥
        System.out.println(RsaUtils.getPrivateKey(privateFilePath));
        //获取公钥
        System.out.println(RsaUtils.getPublicKey(publicFilePath));
    }
}
