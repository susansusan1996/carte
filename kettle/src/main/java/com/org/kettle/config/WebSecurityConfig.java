package com.org.kettle.config;

import com.org.kettle.Filter.HrmBasicAuthenticationFilter;
import com.org.kettle.Filter.HrmUserNamePasswordAuthenticationFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

@Configuration
@EnableGlobalMethodSecurity(prePostEnabled = true) //開啟@preAuthorized註解
@EnableWebSecurity
public class WebSecurityConfig extends WebSecurityConfigurerAdapter {


    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {

        http.csrf().disable()//關閉跨域偽造檢查
                .authorizeRequests()
                .antMatchers("/login").permitAll()
                .anyRequest().authenticated() //其他路徑都要認證之後才能訪問
                .and().formLogin()//允許表單登入
                .loginProcessingUrl("/login")
//                .successHandler(new MyAuthenticationSuccessHandler())
//                .failureHandler(new MyAuthenticationFailureHandler())
                .and().logout().permitAll();
                //将自定义的2个filter重新添加到 Security
        http.addFilterBefore(new HrmUserNamePasswordAuthenticationFilter(authenticationManagerBean()), HrmUserNamePasswordAuthenticationFilter.class); //簽發jwt
        http.addFilterAt(new HrmBasicAuthenticationFilter(authenticationManager()), HrmBasicAuthenticationFilter.class); //驗證jwt
    }
}
