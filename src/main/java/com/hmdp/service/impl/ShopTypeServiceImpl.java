package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.constants.RedisConstants;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryType() {
        // 1.从redis中查询店铺类型缓存
        List<String> typeList = stringRedisTemplate.opsForList().range(RedisConstants.CACHE_SHOPTYPE_KEY, 0, -1);
        // 2.缓存是否存在
        if(typeList != null && !typeList.isEmpty()){
            // 如果redis中存在，直接返回 (List<String> 转为 List<ShopType>)
            List<ShopType> shopTypeList = new ArrayList<>();
            for(String type : typeList){
                ShopType shopType = JSONUtil.toBean(type, ShopType.class);
                shopTypeList.add(shopType);
            }
            return Result.ok(shopTypeList);
        }

        // 3.不存在，从数据库查询
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        // 4.不存在返回错误
        if(shopTypes.isEmpty()){
            return Result.fail("暂无店铺类型数据！");
        }

        // 5.存在，添加到redis中 (List<ShopType> 转为 List<String>)
        List<String> shopTypesJson = shopTypes.stream()
                .map(shopType -> JSONUtil.toJsonStr(shopType))
                .collect(Collectors.toList());

        stringRedisTemplate.opsForList().rightPushAll(RedisConstants.CACHE_SHOPTYPE_KEY, shopTypesJson);
        return Result.ok(shopTypes);
    }
}
