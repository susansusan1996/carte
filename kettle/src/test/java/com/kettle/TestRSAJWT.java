package com.kettle;

import com.org.kettle.util.JwtUtils;
import com.org.kettle.util.RsaUtils;
import org.junit.jupiter.api.Test;
import org.springframework.util.ResourceUtils;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;


public class TestRSAJWT {

    @Test //创建基于RSA的jwt令牌
    public void rsaCreateTest() throws Exception {
        //生成jwt令牌的载荷信息
        Map userinfo = new HashMap() {{ //新写法：jdk1.8新特性
            put("account", "jack");
            put("auth", "a,b,c,d");
        }};
        //获取私钥：将生成的私钥保存的文件转换为私钥路径对象
        String path = ResourceUtils.getFile("classpath:rsa.pri").getPath();
        //构建私钥对象 根据路径转换为私钥对象
        PrivateKey privateKey = RsaUtils.getPrivateKey(path);
        //下面的方法，在生成jwt令牌的信息时，是通过RSA的私钥生成
        String token = JwtUtils.generateTokenExpireInMinutes(userinfo, privateKey, 3);
        System.out.println(token);
        // eyJhbGciOiJSUzI1NiJ9
        // .eyJ1c2VyIjoie1wiYXV0aFwiOlwiYSxiLGMsZFwiLFwiYWNjb3VudFwiOlwiamFja1wifSIsImp0aSI6IlpUQmhPVGMwWmpBdFpESmtaUzAwTkdSaExXRmpOVGd0TVRWbE9UZzVOV0ptWm1GayIsImV4cCI6MTYxOTA1OTUzNH0
        // .gohQiN6Dg9x0FnyHJw1ecJLCjnijSG3mFXYGC52ewH7F-xvuiCY7Hr9DF_NtWAdk0LkNg8NUYECnOQicmgBG6kDw56NqiHwn1qT003U83gGaGfM0_fCrSrx-J-Kx9qnHASvULbFPPymgrp3vI7KdAM89fORk8j43sJ-G4ASbEC3R-BkIvlNjNUK3WEQIQ5Yahk4ckHYQQZY0DpxyI-gsRGBB1bf1x0YBTrtM0XTlR2NcIZ6LPPCm84ck8ADsJxrQlG8VgOHUjrO4JAwpNOOGaSrAz37-zoEz4qDwfEKVE5HD7kXWb33bHjCRbipOdSopGz9uezHZyn1HloApiQiFDA
    }

    @Test //将生成的基于RSA的JWT进行解密，获取载荷的数据
    public void rsaRead() throws Exception {
        //解析token
        String token = "eyJhbGciOiJSUzI1NiJ9.eyJ1c2VyIjoie1wiYXV0aFwiOlwiYSxiLGMsZFwiLFwiYWNjb3VudFwiOlwiamFja1wifSIsImp0aSI6Ik56a3dZemxrWWpZdE1UZ3dOQzAwWVdObExUazROVEF0TmpneU0yRXhOMlZsTkRBdyIsImV4cCI6MTcwNDE2NTI5NH0.JBu0DIt-yXPmWGieteP2HIGL6ZGi2wfHs_rHyNfy0Hw7ABA6ahI0LUteIyrVL_juDI-nWSb_i-Yh0WxGTD5l5M7qP1If3DKzWimoWvf-SAczr_edzgyK2KI9qBUmBzNbjwqPn9QkU0oOyKk-60fenmLQWsCzFNGlsBnRysGFJCizjLPZ2x2fKPKyg5D0_wjWb1CSXk4sZkWa84HgUzNUe931g9XRdYMIlMj2OCLHEBbVDkeWZdw8Kgy4bqXTTi4HLmms04Q05iQMMOydJ64d7cLfT-t_I_3CSRpA1BbqZu0TVB6ayKO2lre-xUD3CmBymXfxoYb52QmZjVzcaHT_TA";
        //获取公钥路径
        String path1 = ResourceUtils.getFile("classpath:rsa.pub").getPath();
        //构建公钥对象
        PublicKey publicKey = RsaUtils.getPublicKey(path1);

        Map infoFromToken = (Map) JwtUtils.getInfoFromToken(token, publicKey, Map.class);
        System.out.println(infoFromToken.get("account"));
        System.out.println(infoFromToken.get("auth"));
    }
}

