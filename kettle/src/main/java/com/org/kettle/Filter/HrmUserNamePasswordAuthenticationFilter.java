package com.org.kettle.Filter;


import com.alibaba.fastjson.JSONObject;
import com.org.kettle.util.JwtUtils;
import com.org.kettle.util.RsaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.util.ResourceUtils;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.PrivateKey;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class HrmUserNamePasswordAuthenticationFilter extends UsernamePasswordAuthenticationFilter {
    private static final Logger log = LoggerFactory.getLogger(HrmUserNamePasswordAuthenticationFilter.class);

    private AuthenticationManager authenticationManager;

    public HrmUserNamePasswordAuthenticationFilter(AuthenticationManager authenticationManager) {
        this.authenticationManager = authenticationManager;
    }

    //这个方法的执行时间：是在security框架调用认证管理器，找自定义的UserDetailService之前调用
    @Override
    public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response) throws AuthenticationException {
        //拿到登錄的用戶和密碼
        try {
            //這樣讀不到request.getInputStream()，因為是用HTML提交表單，而不是用JSON
            //Map map = new ObjectMapper().readValue(request.getInputStream(), Map.class);
            // 要使用 request.getParameter 獲取表單內容
            String username = request.getParameter("username");
            String password = request.getParameter("password");
            //这个方法是提供的一个扩展方法：就是如果前端传递密码时，进行了加密处理，则在此处进行解密
            //如果前端并没有进行加密，此方法可不用再重写
            return authenticationManager.authenticate(
                    new UsernamePasswordAuthenticationToken(username, password));
        } catch (Exception e) {
            log.error("登入失敗:{}", e);
            return null;
        }
    }

    //执行时间：是在UserDetailService执行之后，且认证通过的情况，会执行
    @Override
    protected void successfulAuthentication(HttpServletRequest request, HttpServletResponse response, FilterChain chain, Authentication authResult) throws IOException, ServletException {
//        super.successfulAuthentication(request, response, chain, authResult);
        //生成jwt令牌
        try {
            String userName = authResult.getPrincipal().toString();

            final Collection<? extends GrantedAuthority> authorities = authResult.getAuthorities();
//            final String name = authResult.getName();
            Map userInfo = new HashMap() {{ //jwt payload
                put("user", userName);
                put("auths", authorities);
            }};

            //獲取私鑰
            String path = ResourceUtils.getFile("classpath:rsa.pri").getPath();
            //私鑰
            PrivateKey privateKey = RsaUtils.getPrivateKey(path);
            //jwt有效時間為20分鐘
            String token = JwtUtils.generateTokenExpireInMinutes(userInfo, privateKey, 20);

            //響應給客戶端
            responseErrJson(response, token);

        } catch (Exception e) {
            log.error("登入失敗:{}", e);
            responseErrJson(response, "登入失敗");
        }
    }

    private static void responseErrJson(HttpServletResponse response, String data) {
        response.setContentType("application/json;charset=utf-8");
        PrintWriter out = null;
        try {
            out = response.getWriter();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        out.write(JSONObject.toJSONString(data));
        out.flush();
        out.close();
    }

}

