package com.aliyun.odps.mma.mapper;

import com.aliyun.odps.mma.model.ConfigItem;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface ConfigMapper {
    List<ConfigItem> getAllConfig();
    List<ConfigItem> getCategoryConfig(@Param("category") String category);
    String getConfig(@Param("category") String category, @Param("name") String name);
    void insertConfig(@Param("category") String category, @Param("name") String name, @Param("value") String value);
    void updateConfig(@Param("category") String category, @Param("name") String name, @Param("value") String value);
    void updateCategory(@Param("category") String category, @Param("newCategory") String newCategory);
    void deleteConfig(@Param("category") String category, @Param("name") String name);
}
