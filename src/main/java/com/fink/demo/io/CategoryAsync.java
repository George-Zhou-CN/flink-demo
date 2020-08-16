package com.fink.demo.io;

import com.fink.demo.model.Category;
import com.fink.demo.model.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.ExecutorUtils;

import java.util.Collections;
import java.util.concurrent.*;

/**
 * @Auhtor Jiazhi
 * @Date 2020/7/4 9:10 下午
 **/
public class CategoryAsync extends RichAsyncFunction<UserBehavior, Tuple2<UserBehavior, Category>> {

    private transient ExecutorService executorService;

    private CategoryDimReader categoryDimReader = null;

    public CategoryAsync() {
        super();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        categoryDimReader = new CategoryDimReader();
        executorService = Executors.newFixedThreadPool(30);
    }

    @Override
    public void asyncInvoke(UserBehavior userBehavior, ResultFuture<Tuple2<UserBehavior, Category>> resultFuture) throws Exception {
        Future<Category> result = executorService.submit(() -> categoryDimReader.getCategoryBySubId(userBehavior.getCategoryId()));
        CompletableFuture.supplyAsync(() -> {
            try {
                return result.get();
            } catch (InterruptedException | ExecutionException e) {
                // Normally handled explicitly.
                return null;
            }
        }).thenAccept((Category category) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<>(userBehavior, category)));
        });
    }

    @Override
    public void close() throws Exception {
        super.close();
        ExecutorUtils.gracefulShutdown(20000, TimeUnit.MILLISECONDS, executorService);
    }

    @Override
    public void timeout(UserBehavior input, ResultFuture<Tuple2<UserBehavior, Category>> resultFuture) throws Exception {
        System.out.println("Async function call has timed out. UserBehavior: " + input.toString());
    }
}
