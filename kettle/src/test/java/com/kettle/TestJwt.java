package com.kettle;

import com.alibaba.fastjson.JSONObject;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.org.kettle.service.dto.User;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.jupiter.api.Test;

import java.util.Date;

public class TestJwt {

    @Test
    public void createJwt() {
        String sign = JWT.create()
                //.withHeader()可以不给，默认即可
                //添加jwt的载荷信息，使用的api方法withClaim（），这个方法在添加数据是以 key-value键值对格式进行添加
                //本质上，载荷中的信息类似于一个map类型
                .withClaim("account", "135123123")
                .withClaim("userId", "uuid")
                .withClaim("age", 12)
                //jwt可以对载荷中的信息，进行有效时间的设置。下面的设置表示：当前jwt令牌有效时间：3分钟
                .withExpiresAt(DateUtils.addMinutes(new Date(), 3))
                //设置的值即为  密钥
                .sign(Algorithm.HMAC256("##@@!!"));
        System.out.println(sign);
        //eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9 头部信息经过base64转码之后的字符串
        // .eyJleHAiOjE2MTkwNTc2OTIsInVzZXJJZCI6InV1aWQiLCJhY2NvdW50IjoiMTM1MTIzMTIzIiwiYWdlIjoxMn0 载荷进行base64加密之后字符串
        // .JCagRTvVd_dshxGF214TpbGDNIvsj3Lift9x2ZZ-vZw 签证base64加密之后的字符串
    }

    @Test
    public void verifyJwt() {
        String jwt = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE3MDQwMzIxMDksInVzZXJJZCI6InV1aWQiLCJhY2NvdW50IjoiMTM1MTIzMTIzIiwiYWdlIjoxMn0.S90FmX-S0GRmp1rJWMzoryYJw3omxEiPyWoeMi6ejSE";
        //##@@!! 它就是生成jwt时的密钥
        JWTVerifier jwtVerifier = JWT.require(Algorithm.HMAC256("##@@!!")).build();//先验签
        DecodedJWT verify = jwtVerifier.verify(jwt);
        System.out.println(verify.getClaim("account").asString());
        System.out.println(verify.getClaim("userId").asString());
        System.out.println(verify.getClaim("age").asInt());
    }

    @Test //传对象
    public void createJwt2() {
        //jwt需要保存user对象的信息
        User user = new User();
        user.setUserName("admin");
        user.setPassword("123456");
        user.setAge(22);
        String sign = JWT.create()
                //.withHeader()可以不给，默认即可
                //添加jwt的载荷信息，使用的api方法withClaim（），这个方法在添加数据是以 key-value键值对格式进行添加
                //本质上，载荷中的信息类似于一个map类型
                .withClaim("user", JSONObject.toJSONString(user))
                //jwt可以对载荷中的信息，进行有效时间的设置。下面的设置表示：当前jwt令牌有效时间：3分钟
                .withExpiresAt(DateUtils.addMinutes(new Date(), 3))
                //设置的值即为  密钥
                .sign(Algorithm.HMAC256("##@@!!999"));
        System.out.println(sign);
        //eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9 头部信息经过base64转码之后的字符串
        // .eyJleHAiOjE2MTkwNTc2OTIsInVzZXJJZCI6InV1aWQiLCJhY2NvdW50IjoiMTM1MTIzMTIzIiwiYWdlIjoxMn0 载荷进行base64加密之后字符串
        // .JCagRTvVd_dshxGF214TpbGDNIvsj3Lift9x2ZZ-vZw 签证base64加密之后的字符串
    }

    //
    @Test
    public void verifyJWT2() {
        String jwt = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE3MDQwMzI3OTEsInVzZXIiOiJ7XCJhZ2VcIjoyMixcInBhc3N3b3JkXCI6XCIxMjM0NTZcIixcInVzZXJOYW1lXCI6XCJhZG1pblwifSJ9.T-A97jIuiLlQNrnkZPGFsCFyIa-2D3UwbMDi_HQkuII";
        //##@@!! 它就是生成jwt时的密钥
        JWTVerifier jwtVerifier = JWT.require(Algorithm.HMAC256("##@@!!999")).build();//先验签
        DecodedJWT verify = jwtVerifier.verify(jwt);
        System.out.println(verify.getClaim("user").asString());
    }
}
