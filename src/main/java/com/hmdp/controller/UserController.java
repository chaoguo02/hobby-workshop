package com.hmdp.controller;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.RandomUtil;
import com.hmdp.annotation.RateLimit;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.entity.UserInfo;
import com.hmdp.service.IUserInfoService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

@Slf4j
@RestController
@RequestMapping("/user")
public class UserController {

    @Resource
    private IUserService userService;

    @Resource
    private IUserInfoService userInfoService;

    /**
     * 发送手机验证码
     *
     * <p>按手机号限流：capacity=1 表示桶里最多 1 个令牌，rate=1/60 表示每 60 秒补 1 个，
     * 合起来就是「同一手机号 60 秒内只能发一次」。这个接口是免登录的，ThreadLocal 里没有用户，
     * 所以维度只能取入参 phone。
     */
    @RateLimit(keyPrefix = "sms:send", key = "#phone", capacity = 1, rate = 1.0 / 60,
            message = "验证码发送过于频繁，请 1 分钟后再试")
    @PostMapping("code")
    public Result sendCode(@RequestParam("phone") String phone, HttpSession session) {
        return userService.sendCode(phone, session);
    }

    /**
     * 登录功能
     * @param loginForm 登录参数，包含手机号、验证码；或者手机号、密码
     */
    @PostMapping("/login")
    public Result login(@RequestBody LoginFormDTO loginForm, HttpSession session){
        // 实现登录功能
        return userService.login(loginForm, session);
    }

    /**
     * 登出功能
     * @return 无
     */
    @PostMapping("/logout")
    public Result logout(){
        // TODO 实现登出功能
        return Result.fail("功能未完成");
    }

    @GetMapping("/me")
    public Result me(){
        // 获取当前登录的用户并返回
        UserDTO userDTO = UserHolder.getUser();
        return Result.ok(userDTO);
    }

    @GetMapping("/info/{id}")
    public Result info(@PathVariable("id") Long userId){
        // 查询详情
        UserInfo info = userInfoService.getById(userId);
        if (info == null) {
            // 没有详情，应该是第一次查看详情
            return Result.ok();
        }
        info.setCreateTime(null);
        info.setUpdateTime(null);
        // 返回
        return Result.ok(info);
    }

    @GetMapping("/{id}")
    public Result queryUserById(@PathVariable("id") Long userId){
        // 查询详情
        User user = userService.getById(userId);
        if (user == null) {
            return Result.ok();
        }
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        // 返回
        return Result.ok(userDTO);
    }

    @PostMapping("/sign")
    public Result sign(){
        return userService.sign();
    }

    @GetMapping("/sign/count")
    public Result signCount(){
        return userService.signCount();
    }
}
