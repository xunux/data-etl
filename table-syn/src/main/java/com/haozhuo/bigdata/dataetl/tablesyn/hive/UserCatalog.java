package com.haozhuo.bigdata.dataetl.tablesyn.hive;

import com.haozhuo.bigdata.dataetl.JavaUtils;
import com.haozhuo.bigdata.dataetl.bean.User;
import com.haozhuo.bigdata.dataetl.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by LingXin on 6/27/17.
 */
public class UserCatalog implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(UserCatalog.class);

    public static void insert(List<User> users) {
        if (users.size() == 0)
            return;
        logger.info("{}个users开始插入Hive", users.size());
        ArrayList<String> partitionVals = new ArrayList<String>(1);
        partitionVals.add(JavaUtils.getStrDate("yyyy-MM-dd"));

        HiveCatalog hiveCatalog = new HiveCatalog("user_info", new String[]{"user_id", "mobile", "sex", "is_married", "has_born", "device_model", "city", "last_update_time", "birthday","id_card_no_md5"}, partitionVals);
        List<String> userInfos = new ArrayList<String>(users.size());
        for (User user : users) {
            userInfos.add(new StringBuffer().append(user.getUserId()).append(HiveCatalog.delimiter)
                    .append(user.getMobile()).append(HiveCatalog.delimiter)
                    .append(user.getSex()).append(HiveCatalog.delimiter)
                    .append(user.getIsMarried()).append(HiveCatalog.delimiter)
                    .append(user.getHasBorn()).append(HiveCatalog.delimiter)
                    .append(user.getDeviceModel()).append(HiveCatalog.delimiter)
                    .append(user.getCity()).append(HiveCatalog.delimiter)
                    .append(user.getLastUpdateTime()).append(HiveCatalog.delimiter)
                    .append(user.getBirthday()).append(HiveCatalog.delimiter)
                    .append(user.getIdCardNoMd5()).append(HiveCatalog.delimiter)
                    .toString());
        }
        hiveCatalog.save(userInfos, 2);
        logger.info("users插入Hive完成", users.size());
    }
}
