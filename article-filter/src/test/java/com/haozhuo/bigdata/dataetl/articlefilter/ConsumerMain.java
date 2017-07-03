/*package com.haozhuo.bigdata.dataetl.articlefilter;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.haozhuo.bigdata.dataetl.bean.Article;
import com.haozhuo.bigdata.dataetl.beans.WrapService;


*//**
 * Created by LingXin on 6/19/17.
 *//*
public class ConsumerMain {

    public static void main(String[] args) {
        ApplicationConfig application = new ApplicationConfig();
        application.setName("WrapServiceConsumer");

        // 连接注册中心配置
        RegistryConfig  registry= new RegistryConfig();
        registry.setAddress("zookeeper://192.168.1.150:2181");
        registry.setTimeout(100000);

        // 引用远程服务
        ReferenceConfig<WrapService> reference  = new ReferenceConfig<WrapService>(); // 此实例很重，封装了与注册中心的连接以及与提供者的连接，请自行缓存，否则可能造成内存和连接泄漏
        reference.setApplication(application);
        reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
        reference.setInterface(WrapService.class);
        reference.setVersion("1.0"); //这个一定要与provider的version一致

        WrapService wrap = reference.get(); // 注意：此代理对象内部封装了所有通讯细节，对象较重，请缓存复用
        Article[] articles = new Article[1];
        articles[0] = new Article(0,0,null,null,null,null,null,null,null,null,null,null,0);
        if(wrap== null) {
            System.out.println("xx");
        } else {
            System.out.println(wrap.foo(articles).get(0).display_url());
        }
    }
}*/
