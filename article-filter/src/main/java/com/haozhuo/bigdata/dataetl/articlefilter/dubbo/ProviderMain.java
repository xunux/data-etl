package com.haozhuo.bigdata.dataetl.articlefilter.dubbo;

//import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.haozhuo.bigdata.dataetl.Props;
import com.haozhuo.common.sys.util.DateUtil;
import com.haozhuo.info.dubbo.InfoModuleService;
import com.haozhuo.info.entity.vo.param.InformationBigDataParam;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by LingXin on 6/19/17.
 */
public class ProviderMain {
    public static void main(String[] args) {
        ApplicationConfig application =  new ApplicationConfig();
        application.setName("cxxx");

        // 连接注册中心配置
        RegistryConfig registry  = new RegistryConfig();
        //registry.setProtocol("zookeeper")
        registry.setAddress("zookeeper://192.168.1.141:2181");
        registry.setTimeout(10000);

        // 注意：ReferenceConfig为重对象，内部封装了与注册中心的连接，以及与服务提供方的连接

        // 引用远程服务
        ReferenceConfig<InfoModuleService> reference = new ReferenceConfig<InfoModuleService>(); // 此实例很重，封装了与注册中心的连接以及与提供者的连接，请自行缓存，否则可能造成内存和连接泄漏
        reference.setApplication(application);
        reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
        reference.setInterface(InfoModuleService.class);
        // reference.setVersion(Props.get("dubbo.reference.wrapservce.version")) //这个一定要与provider的version一致
        InfoModuleService ims = reference.get(); // 注意：此代理对象内部封装了所有通讯细节，对象较重，请缓存复用
        List<InformationBigDataParam> list = new ArrayList<InformationBigDataParam>();
        InformationBigDataParam info = new InformationBigDataParam();
        info = new InformationBigDataParam();
        info.setFingerprint(111L);
        info.setType((byte) 0);
        info.setStatus((byte) 0);
        info.setUrlType((byte) 0);
        info.setTitle("基因科技携手健康保险 合力打造健康命运共同体" );
        info.setImage("http://article.h5.ihaozhuo.com/2017/04/27/1493261878603.html");
        info.setImages("http://article.h5.ihaozhuo.com/2017/04/27/1493261878603.html");
        info.setContent("2017年5月18日，“健康上海·让生命遇见关爱”——泰康健康关爱特定疾病保险产品发布会在上海举行。华大基因联手泰康养老保险、长生树健康在沪正式推出保险市场上首款“基因检测+健康管理+保险保障”的防癌险产品，合力打造上海市民健康命运共同体。");
        info.setDetail("<p>2017年5月18日，“健康上海·让生命遇见关爱”——泰康健康关爱特定疾病保险产品发布会在上海举行。<strong>华大基因联手泰康养老保险、长生树健康在沪正式推出保险市场上首款“基因检测+健康管理+保险保障”的防癌险产品</strong>，合力打造上海市民健康命运共同体。</p><p><img alt=\"基因科技携手健康保险 合力打造健康命运共同体\" img_height=\"601\" img_width=\"900\" inline=\"0\" src=\"http://article.image.ihaozhuo.com/2017/06/06/14967565839466904.png\"/></p><p class=\"pgc-img-caption\">泰康健康关爱特定疾病保险产品发布会</p><p><strong>个性化设计</strong></p><p><strong>一站式防癌解决方案</strong></p><p>此次推出的泰康健康关爱特定疾病保险与传统保险产品不同，它不仅仅停留在事后理赔，而是依托上海已有的分级诊疗体系和家庭医生制度，通过精准介入医疗过程，<strong>满足个性化的健康需求，提供早筛查、早发现、优治疗、享保障的一站式预防癌症解决方案，实现了由理赔服务向健康服务的转变，真正变保险为保健</strong>。</p><p>该产品保障范围包括男性7种、女性9种发病率最高的肿瘤，涵盖胃癌、肠癌、肝癌、肺癌、膀胱癌、食管癌、前列腺癌、乳腺癌、卵巢癌、宫颈癌，投保范围从18岁起，最大可至79岁的高龄人群。一期产品目前只针对上海地区有社保的健康人群，二期将开放团体购买并对既往症人群开放参保，未来有望覆盖非社保人群。<strong>客户将享受到特定肿瘤的咨询、筛查、诊断、治疗、保险理赔等综合服务，该产品内嵌专属健康档案、肿瘤基因检测、家庭医生服务、健康问卷咨询等服务。肿瘤疑似患者将由家庭医生安排转诊至专科或综合性三甲医院就诊，可享受专科医生提供的门诊或住院治疗服务，由保险金弥补医疗费支出。</strong></p><p><strong>华大基因为客户提供了5种遗传性肿瘤基因检测（遗传性乳腺癌、卵巢癌、胃癌、结直肠癌、前列腺癌）和人乳头瘤病毒HPV分型基因检测。</strong>遗传性肿瘤基因检测采用目标区域捕获结合高通量测序技术，检测范围包括涉及基因的全部外显子区和部分内含子区。通过分析易感基因变异情况，帮助了解自身是否携带遗传性肿瘤相关基因，提供肿瘤早期筛查以及风险评估依据。</p><p>2015年3月27日，深圳华大临床检验中心和天津华大医学检验所双双入选国家卫计委首批肿瘤高通量基因测序技术临床应用试点单位，均满分通过2015全国肿瘤诊断与治疗高通量测序检验（体细胞突变和胚系突变）室间质评；2016年5月，双机构全部满分通过BRAF、EGFR、KRAS、PIK3CA突变、UGT1A1多态性、肿瘤游离DNA（ctDNA）基因突变检测等室间质评；2016年8月，深圳华大临床检验中心经美国病理学家协会（COLLEGE OF AMERICAN PATHOLOGISTS，CAP）认证其BRCA1/2基因检测能力验证全部合格，获得全球最高水准的临床实验室认可；<strong>2016年10月，华大基因自主研发测序仪BGISEQ-500获国家食品药品监督管理总局医疗器械注册证；2016年11月，国内首款非小细胞肺癌突变基因分析软件注册证获批上市。</strong></p><p><strong>科技造福</strong></p><p><strong>基因检测人人可及，人人可享</strong></p><p>目前我国80%的癌症患者确诊时即属于中晚期，而在日本，大约80%的癌症发现处于早初期，其中80%的人得到治愈，癌症治愈率高达68%，位列世界第一。</p><p><strong>作为全球领先的基因科技研发和产业机构，经过18年的不懈努力，华大基因践行基础研究、产业应用和人才教育的协同发展，分支机构遍布全球60多个国家和地区，其组建运营的深圳国家基因库于2016年9月正式投入使用，是我国唯一一个获批的国家级基因库。</strong>目前，国家基因库已初步建成了 “三库两平台”（样本库、数据库、活体库、基因测序平台、基因合成编辑平台）的业务架构，已保存约1000万份生物样本，构建了40多个基因数据库。华大基因已在全国19个省开展了基因检测全覆盖的民生项目实践，让基因检测人人可及，人人可享。</p><p><img alt=\"基因科技携手健康保险 合力打造健康命运共同体\" img_height=\"601\" img_width=\"900\" inline=\"0\" src=\"http://article.image.ihaozhuo.com/2017/06/06/14967565841767709.png\"/></p><p class=\"pgc-img-caption\">华大基因保险事业部副总经理靳大卫发言</p><p>未来，凭借全球领先的基因组学研究能力，随着测序成本不断下降，<strong>华大基因将把基因检测技术更多的应用到健康领域，人类将迈入个人基因组时代，为解释重大疾病发病机理、开展个性化预测、预防和治疗打下科学基础，实现精准预测、精准诊断、精准治疗和精准康复。 </strong></p><p><img alt=\"基因科技携手健康保险 合力打造健康命运共同体\" img_height=\"601\" img_width=\"900\" inline=\"0\" src=\"http://article.image.ihaozhuo.com/2017/06/06/14967565843644884.png\"/></p><p class=\"pgc-img-caption\">泰康健康关爱特定疾病保险产品发布会现场合影</p><p>本次发布会出席的嘉宾有上海市保险同业公会副秘书长伍国良、泰康养老保险股份有限公司副总经理、总精算师陈兵、上海分公司总经理王卫群、产品精算部副总经理李科、华大基因保险事业部副总经理靳大卫、华大基因股份有限公司大客户部保险业务总监肖若、上海长生树健康管理有限公司董事长秦懿、副总经理李茜等。</p><p>本信息来源于华大基因公众号</p>");
        info.setStartTime(DateUtil.parseDatetime("2017-06-09 16:25:07"));
        info.setCreateTime(DateUtil.parseDatetime("2017-06-09 14:40:39"));
        info.setLastUpdateTime(DateUtil.parseDatetime("2017-06-09 14:40:39"));
        info.setSource("");
        list.add(info);
        Map<Long,Integer> a = ims.addInformationByBigData(list);
        System.out.println(a);
    }
/*    public static void main(String[] args) {
        try {
            ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] { "classpath:spring-beans.xml" });
            context.start();
            System.out.println("按任意键退出");
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }*/
}
