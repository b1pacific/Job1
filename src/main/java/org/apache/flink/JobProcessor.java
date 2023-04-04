package org.apache.flink;

import com.google.common.base.Strings;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Query;

import java.util.Map;
import java.util.Optional;

public class JobProcessor extends ProcessFunction<StringObjectMap, StringObjectMap> {

    public JobProcessor() {
    }
    @Override
    public void processElement(StringObjectMap value, ProcessFunction<StringObjectMap, StringObjectMap>.Context ctx, Collector<StringObjectMap> out) throws Exception {

        System.out.println("Hello from here");

        Jdbi jdbi = Jdbi.create("jdbc:mysql://localhost:3306/test", "root", "");

        jdbi.useHandle(handle -> {
            Query q1 = handle.createQuery("select mrks from tb");
//            Query q2 = handle.createQuery("update tb set mrks = :mk where id = :id");

            Optional<Map<String, Object>> first = q1.mapToMap().findFirst();

            System.out.println("This is the result " + first);
        });


        System.out.println(value);
    }
}
