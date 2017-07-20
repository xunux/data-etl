package com.haozhuo.info.dubbo;

import com.haozhuo.info.entity.vo.param.InformationBigDataParam;

import java.util.List;
import java.util.Map;


/**
 * Created by gk on 16/11/16.
 */
public interface InfoModuleService {


    /**
     * 删除重复资讯
     */
    void deleteInformationByBigData(List<String> informationIds) throws Exception;

    Map<Long, Integer> addInformationByBigData(List<InformationBigDataParam> param);

}
