package com.org.kettle.Filter;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.org.kettle.util.JwtUtils;
import com.org.kettle.util.RsaUtils;
import org.apache.tomcat.util.buf.StringUtils;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.util.ResourceUtils;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.util.*;

public class HrmBasicAuthenticationFilter extends BasicAuthenticationFilter {


    public HrmBasicAuthenticationFilter(AuthenticationManager authenticationManager) {
        super(authenticationManager);
    }

    /**
     * 校验token  这个方法：是每次前端发起请求时，都会进入此处。
     * 所以：可以在此方法中，进行jwt的解密
     */
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) {

        //从请求的header头信息中来获取token
        //Authorization:就是前端在header头部传递令牌时的key名。这个变量名可以随便定义
        String header = request.getHeader("Authorization");//得到的就是前端传递的token字符串
        if (header == null) {//如果没有令牌
            try {
                response.setContentType("application/json;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                PrintWriter out = response.getWriter();
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("code", HttpServletResponse.SC_FORBIDDEN);
                map.put("message", "请登录！");
                out.write(new ObjectMapper().writeValueAsString(map));
                out.flush();
                out.close();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            return;
        }
        //从token中将用户的权限解析出来放在springsecurity上下文中
        //将权限放入上下文的目的是，后续访问资源的时候能判断该用户是否有该权限
        try {
            //下面这2步操作：类似 【狸猫换太子】
            //校验token是否有效,理由：可能令牌字符串不对，或者令牌失效（过期）
            UsernamePasswordAuthenticationToken authentication = getAuthentication(request);
            SecurityContextHolder.getContext().setAuthentication(authentication);
            //放行
            chain.doFilter(request, response);
        } catch (Exception e) {
            try {
                response.setContentType("application/json;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                PrintWriter out = response.getWriter();
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("code", HttpServletResponse.SC_FORBIDDEN);
                map.put("message", "请登录！");
                out.write(new ObjectMapper().writeValueAsString(map));
                out.flush();
                out.close();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }

    /**
     * 解析token，获取用户信息
     *
     * @param request
     * @return
     */
    private UsernamePasswordAuthenticationToken getAuthentication(HttpServletRequest request) throws Exception {
        String token = request.getHeader("Authorization");
        if (token != null) {
            //通过token解析出载荷信息
            Map map = (Map) JwtUtils.getInfoFromToken(token, RsaUtils.getPublicKey(ResourceUtils.getFile("classpath:rsa.pub").getPath()),
                    Map.class);

            //取出token中的权限信息
            List<Map> auths = (List) map.get("auths");

            //定义一个临时的集合
            List auth_tmp = new ArrayList();

            for (Map auth : auths) {
                auth_tmp.add(auth.get("authority"));
            }
            //封装成权限集合
            Collection<? extends GrantedAuthority> grantedAuthorities =
                    AuthorityUtils.commaSeparatedStringToAuthorityList(StringUtils.join(auth_tmp, ','));
            //解析出jwt令牌中携带的用户权限列表，然后将其重写按钮security的要求，封装为security能识别的token令牌
            return new UsernamePasswordAuthenticationToken(null, null, grantedAuthorities);
        }
        return null;
    }
}

