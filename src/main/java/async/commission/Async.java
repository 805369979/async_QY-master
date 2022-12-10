package async.commission;

import async.commission.entity.Context;
import async.commission.template.AbstractNode;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 并行任务门面
 * 线程池数量可以根据自己情况调整core线程的数量
 */
public class Async<T, V> {
    /**
     * 默认不定长线程池
     */
    private static final ThreadPoolExecutor COMMON_POOL = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    /**
     * 注意，这里是个static，也就是只能有一个线程池。用户自定义线程池时，也只能定义一个
     */
    private static ExecutorService executorService;

    public static Map<String, AbstractNode> startWork(long timeout, Context context, ExecutorService executorService, List<AbstractNode> nodes) {
        if (nodes == null || nodes.size() == 0) {
            return new ConcurrentHashMap<>();
        }
        //保存线程池变量
        Async.executorService = executorService;
        //定义一个map，存放所有的node，key为wrapper的唯一id，value是该node，可以从value中获取node的result
        ConcurrentHashMap<String, AbstractNode> forParamUseWrappers = new ConcurrentHashMap<>();
        CompletableFuture[] count = new CompletableFuture[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            AbstractNode node = nodes.get(i);
            count[i] = CompletableFuture.runAsync(()->{
            node.template(executorService,node, timeout, forParamUseWrappers,context);
        },executorService);
         }
        try {
            CompletableFuture.allOf(count).get(timeout, TimeUnit.MILLISECONDS);
            System.out.println("执行完成");
        } catch (Exception e) {
            Set<AbstractNode> set = new HashSet<>();
            totalWorkers(nodes, set);
            for (AbstractNode node : set) {
                node.stopNow();
            }
            return forParamUseWrappers;
        }
        return forParamUseWrappers;
    }
    /**
     * 如果想自定义线程池，请传pool，不自定义的话，就走默认的COMMON_POOL
     */
    public static Map<String, AbstractNode> startWork(long timeout, Context context, ExecutorService executorService, AbstractNode... nodes) throws ExecutionException, InterruptedException {
        if (nodes == null || nodes.length == 0) {
            return new ConcurrentHashMap<>();
        }
        List<AbstractNode> nodeList = Arrays.stream(nodes).collect(Collectors.toList());
        return startWork(timeout, context, executorService, nodeList);
    }

    /**
     * 同步阻塞,直到所有都完成,或失败
     */
    public static Map<String, AbstractNode> startWork(long timeout, Context context, AbstractNode... nodes) throws ExecutionException, InterruptedException {
        return startWork(timeout,context, COMMON_POOL, nodes);
    }
    /**
     * 总共多少个执行单元
     */
    @SuppressWarnings("unchecked")
    private static void totalWorkers(List<AbstractNode> abstractNodes, Set<AbstractNode> set) {
        set.addAll(abstractNodes);
        for (AbstractNode node : abstractNodes) {
            if (node.getSonHandler() == null) {
                continue;
            }
            List<AbstractNode> wrappers = node.getSonHandler();
            totalWorkers(wrappers, set);
        }
    }

    /**
     * 关闭线程池
     */
    public static void shutDown() {
        shutDown(executorService);
    }

    /**
     * 关闭线程池
     */
    public static void shutDown(ExecutorService executorService) {
        if (executorService != null) {
            executorService.shutdown();
        } else {
            COMMON_POOL.shutdown();
        }
    }

    public static String getThreadCount() {
        return "activeCount=" + COMMON_POOL.getActiveCount() +
                "  completedCount " + COMMON_POOL.getCompletedTaskCount() +
                "  largestCount " + COMMON_POOL.getLargestPoolSize();
    }
}
