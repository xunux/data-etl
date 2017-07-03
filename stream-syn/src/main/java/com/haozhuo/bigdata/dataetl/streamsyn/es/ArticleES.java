package com.haozhuo.bigdata.dataetl.streamsyn.es;


import com.haozhuo.bigdata.dataetl.Props;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.Serializable;

/**
 * Created by LingXin on 6/29/17.
 * https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/transport-client.html
 */
public class ArticleES implements Serializable {
    private static String esIndexArticle = Props.get("es.index.article");
    private static String esTypeArticle = Props.get("es.type.article");
    private static final Logger logger = LoggerFactory.getLogger(ArticleES.class);

    public static void delete(Long[] docIds) throws Exception {
        TransportClient client = EsUtils.getClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (Long docId : docIds) {
            bulkRequest.add(client.prepareDelete(esIndexArticle, esTypeArticle, docId.toString()));
        }
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            logger.info("EsUtils bulk response has failures", bulkResponse.buildFailureMessage());
        } else {
            logger.info("ES delete {} 个文档", docIds.length);
        }
    }
}
