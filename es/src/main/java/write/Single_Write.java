package write;

import bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class Single_Write {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取客户端连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.将单条数据写入ES

        Movie movie = new Movie("103", "功夫");
        Index index = new Index.Builder(movie)
                .index("movie")
                .type("_doc")
                .id("1003")
                .build();
//        Index index = new Index.Builder("{\n" +
//                "  \"id\":\"102\",\n" +
//                "  \"name\":\"叶问\"\n" +
//                "}")
//                .index("movie")
//                .type("_doc")
//                .id("1002")
//                .build();
        jestClient.execute(index);

        //关闭连接
        jestClient.shutdownClient();
    }
}
