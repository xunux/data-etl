package com.haozhuo.bigdata.dataetl.articlefilter.es;

import com.haozhuo.bigdata.dataetl.Props;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by LingXin on 6/29/17.
 */
public class EsUtils implements Serializable {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EsUtils.class);
    private static TransportClient client;

    public static synchronized TransportClient getClient() throws Exception {
        if (client == null || client.connectedNodes() == null || client.connectedNodes().size() == 0) {
            try {
                String clusterName = Props.get("es.cluster.name");
                Settings settings = Settings.builder()
                        .put("cluster.name", clusterName).build();
                String[] esNodes = Props.get("es.nodes").split(",");
                client = new PreBuiltTransportClient( settings);
                for (String node : esNodes) {
                    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(node), 9300));
                }
           } catch (UnknownHostException e) {
                logger.error("UnknownHostException", e);
            }
            logger.info("初始化ES的TransportClient");
        }
        return client;
    }
}
