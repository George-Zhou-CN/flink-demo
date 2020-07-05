package com.fink.demo.io;

import com.fink.demo.model.Category;

import java.util.List;
import java.util.Map;

/**
 * @Auhtor Jiazhi
 * @Date 2020/7/4 8:58 下午
 **/
public class CategoryDimReader {

    public Category getCategoryBySubId(Long subId) throws Exception {
        MysqlOperator mysqlOperator = new MysqlOperator();
        List<Map<String, Object>> rows = mysqlOperator.query("select * from category where sub_category_id = " + subId);
        if (rows == null || rows.isEmpty()) {
            return null;
        }

        Category category = new Category();
        Map<String, Object> row = rows.get(0);
        for (Map.Entry<String, Object> column : row.entrySet()) {
            switch (column.getKey()) {
                case "sub_category_id":
                    category.setSubCategoryId((Long) row.get("sub_category_id"));
                    continue;
                case "parent_category_id":
                    category.setParentCategoryId((Long) row.get("parent_category_id"));
            }
        }
        return category;
    }
}
