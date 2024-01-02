package com.org.kettle.config;

import com.alibaba.fastjson.JSON;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.web.access.AccessDeniedHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class DefaultAccessDeniedHandler implements AccessDeniedHandler {
    @Override
    public void handle(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, AccessDeniedException e) throws IOException, ServletException {
        // 添加你的處理邏輯，例如重定向到一個自定義的錯誤頁面
//        httpServletResponse.sendRedirect("/error/access-denied");

        // 或者你可以返回一個錯誤消息
         httpServletResponse.getWriter().write("Access Denied: " + e.getMessage());

        // 如果你希望返回特定的 HTTP 狀態碼，也可以這樣做
        // httpServletResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
    }
}
