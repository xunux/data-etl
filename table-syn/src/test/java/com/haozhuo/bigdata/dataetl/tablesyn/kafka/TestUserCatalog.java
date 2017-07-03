package com.haozhuo.bigdata.dataetl.tablesyn.kafka;

import com.haozhuo.bigdata.dataetl.bean.User;
import com.haozhuo.bigdata.dataetl.tablesyn.hive.UserCatalog;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by LingXin on 6/27/17.
 */
public class TestUserCatalog {
    public static void main(String[] args) {
        User user = new User();
        user.setBirthday("2017-11-19");
        user.setCity("杭州");
        user.setDeviceModel("iphone");
        user.setHasBorn(1);
        user.setIsMarried(1);
        user.setLastUpdateTime("11");
        user.setMobile("shouji");
        user.setSex("1");
        user.setUserId("5102");
        List<User> list = new ArrayList<User>();
        list.add(user);
        UserCatalog.insert(list);
    }
}
