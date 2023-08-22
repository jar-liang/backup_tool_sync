package top.jarhub.backup.app.interceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.HandlerInterceptor;
import top.jarhub.backup.app.config.ConfigUtils;
import top.jarhub.backup.app.pojo.User;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ValidateInterceptor implements HandlerInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateInterceptor.class);

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String servletPath = request.getServletPath();
        if (servletPath == null) {
            return false;
        }
        LOG.info("request path: {}", servletPath);
        if (servletPath.startsWith("/test")) {
            return true;
        }
        Map<String, String[]> parameterMap = request.getParameterMap();
        if (parameterMap.containsKey("userName") && parameterMap.containsKey("password")) {
            String[] userNames = parameterMap.get("userName");
            String[] passwords = parameterMap.get("password");
            if (userNames == null || passwords == null) {
                return false;
            }
            if (userNames.length != 1 && passwords.length != 1) {
                return false;
            }

            // 判断是否存在用户名已经正确的密码
            List<User> users = ConfigUtils.getUsers();
            boolean authFlag = users.stream().anyMatch(e -> e.getValidFlag() && Objects.equals(e.getName(), userNames[0]) && Objects.equals(e.getPassword(), passwords[0]));
            if (!authFlag) {
                LOG.warn("auth failed info: name={}", userNames[0]);
            }
            return authFlag;
        }
        return false;
    }
}
