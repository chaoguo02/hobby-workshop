package com.hmdp.interceptor;

import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * 秒杀监控、人工处置与工作坊核销端点（/monitor/**）的管理员校验。
 *
 * 项目没有后台角色体系，这里用一个只有运维/内部掌握的令牌（请求头 {@value #TOKEN_HEADER}）作为准入，
 * 与登录拦截器叠加：既要登录、又要令牌，普通用户无法调用批量重放等处置接口。
 * 令牌通过配置 {@code seckill.monitor.admin-token}（环境变量 MONITOR_ADMIN_TOKEN）提供；
 * 未配置时一律拒绝（fail closed），避免默认放行。
 *
 * 说明：Nginx 反代后所有请求的 remoteAddr 都是 127.0.0.1，按来源 IP 限制形同虚设，故用令牌而非回环判断。
 */
public class MonitorAdminInterceptor implements HandlerInterceptor {

    public static final String TOKEN_HEADER = "X-Monitor-Token";

    private final String adminToken;

    public MonitorAdminInterceptor(String adminToken) {
        this.adminToken = adminToken;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String presented = request.getHeader(TOKEN_HEADER);
        if (adminToken == null || adminToken.isEmpty() || presented == null
                || !MessageDigest.isEqual(adminToken.getBytes(StandardCharsets.UTF_8),
                        presented.getBytes(StandardCharsets.UTF_8))) {
            response.setStatus(403);
            return false;
        }
        return true;
    }
}
