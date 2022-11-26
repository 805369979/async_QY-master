package async.commission.template;

import async.commission.callback.ICallback;
import async.commission.callback.IWorker;
import async.commission.entity.Context;
import async.commission.entity.WorkResult;
import async.commission.enums.ResultState;
import async.commission.exception.SkippedException;
import async.commission.executor.timer.SystemClock;
import async.commission.util.RetryUtil;
import lombok.Getter;
import lombok.Setter;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractNode<T,V> implements IWorker<T,V>, ICallback<T,V> {
    @Getter
    @Setter
    public boolean must = true;
    // 节点的名字
    @Getter
    @Setter
    protected String taskName;
    // 节点执行结果状态
    ResultState status = ResultState.DEFAULT;
    //将来要处理的param参数
    @Getter
    @Setter
    protected T param;
    @Getter
    @Setter
    protected String choose;
    @Getter
    @Setter
    protected int retryTimes = 3;
    /**
     * 是否在执行自己前，去校验nextWrapper的执行结果<p>
     * 1   4
     * -------3
     * 2
     * 如这种在4执行前，可能3已经执行完毕了（被2执行完后触发的），那么4就没必要执行了。
     * 注意，该属性仅在nextWrapper数量<=1时有效，>1时的情况是不存在的
     */
    protected volatile boolean needCheckNextWrapperResult = true;
    // 节点状态
    private static final int FINISH = 1;
    private static final int ERROR = 2;
    private static final int WORKING = 3;
    private static final int INIT = 0;
    /**
     * 标记该事件是否已经被处理过了，譬如已经超时返回false了，后续rpc又收到返回值了，则不再二次回调
     * 经试验,volatile并不能保证"同一毫秒"内,多线程对该值的修改和拉取
     * <p>
     * 1-finish, 2-error, 3-working
     */
    @Getter
    private AtomicInteger state = new AtomicInteger(0);
    // 默认节点执行结果
    @Getter
    @Setter
    private volatile WorkResult<V> workResult = WorkResult.defaultResult(nodeName());
    // 该map存放所有的节点名字和节点的映射
    @Getter
    private ConcurrentHashMap<String, AbstractNode<T,V>> forParamUserMap;
    // 依赖的父节点
    @Getter
    protected List<AbstractNode> fatherHandler = new ArrayList<>();
    // 下游的子节点
    @Getter
    protected List<AbstractNode> sonHandler = new ArrayList<>();

    // 给节点起名字
    public abstract String nodeName();
    public void setSonHandler(List<AbstractNode> nodes){
        this.sonHandler = nodes;
    }
    public void setFatherHandler(List<AbstractNode> nodes){
        this.fatherHandler = nodes;
    }
    public void setSonHandler(AbstractNode ...nodes){
        ArrayList<AbstractNode> nodeList = new ArrayList<>();
        for (AbstractNode node : nodes){
            //adding to list
            nodeList.add(node);
        }
        setSonHandler(nodeList);
    }
    public void setFatherHandler(AbstractNode ...nodes){
        ArrayList<AbstractNode> nodeList = new ArrayList<>();
        for (AbstractNode node : nodes){
            //adding to list
            nodeList.add(node);
        }
        setFatherHandler(nodeList);
    }
    // 将自己注册进全局map中
    public void register(ConcurrentHashMap<String, AbstractNode<T,V>> forParamUserMap) {
        // 设置节点名称
        String nodeName = nodeName();
        this.setTaskName(nodeName);
        this.forParamUserMap = forParamUserMap;
        forParamUserMap.put(this.taskName, this);
    }
    // 任务入口函数
    public void template(ExecutorService executorService, AbstractNode fromWrapper, long remainTime,
                         ConcurrentHashMap<String, AbstractNode<T,V>> forParamUseNodes,
                         Context context) {
        // 将注册自己进map中
        register(forParamUseNodes);
        long now = SystemClock.now();
        if (remainTime <= 0) {
            fastFail(INIT, null);
            runSonHandler(context,executorService, remainTime, now);
            return;
        }
        //如果自己已经执行过了。
        //可能有多个依赖，其中的一个依赖已经执行完了，并且自己也已开始执行或执行完毕。当另一个依赖执行完毕，又进来该方法时，就不重复处理了
        if (getState() == FINISH || getState() == ERROR) {
            runSonHandler(context,executorService, remainTime, now);
            return;
        }

        //如果在执行前需要校验nextWrapper的状态
        if (needCheckNextWrapperResult) {
            //如果自己的next链上有已经出结果或已经开始执行的任务了，自己就不用继续了
            if (!checkNextWrapperResult()) {
                fastFail(INIT, new SkippedException());
                runSonHandler(context,executorService, remainTime, now);
                return;
            }
        }

        //如果没有任何依赖，说明自己就是第一批要执行的
        if (fatherHandler == null || fatherHandler.size() == 0) {
            fire(context);
            runSonHandler(context,executorService, remainTime, now);
            return;
        }

        /*如果有前方依赖，存在两种情况
         一种是前面只有一个wrapper。即 A  ->  B
        一种是前面有多个wrapper。A C D ->   B。需要A、C、D都完成了才能轮到B。但是无论是A执行完，还是C执行完，都会去唤醒B。
        所以需要B来做判断，必须A、C、D都完成，自己才能执行 */
        //只有一个依赖
        if (fatherHandler.size() == 1) {
            doDependsOneJob(fromWrapper,context);
            runSonHandler(context,executorService, remainTime, now);
        } else {
            //有多个依赖时
            doDependsJobs(executorService, fromWrapper, fatherHandler, now, remainTime,context);
        }
    }

    private void doDependsOneJob(AbstractNode dependWrapper,Context context) {
        if (ResultState.TIMEOUT == dependWrapper.getWorkResult().getResultState()) {
            workResult = defaultResult();
            fastFail(INIT, null);
        } else if (ResultState.EXCEPTION == dependWrapper.getWorkResult().getResultState()) {
            workResult = defaultExResult(dependWrapper.getWorkResult().getEx());
            fastFail(INIT, null);
        } else {
            //前面任务正常完毕了，该自己了
            fire(context);
        }
    }
    private synchronized void doDependsJobs(ExecutorService executorService,
                                            AbstractNode fromWrapper, List<AbstractNode> dependWrappers,
                                            long now, long remainTime,Context context) {
        //如果当前任务已经完成了，依赖的其他任务拿到锁再进来时，不需要执行下面的逻辑了。
        if (getState() != INIT) {
            return;
        }
        boolean nowDependIsMust = false;
        //创建必须完成的上游wrapper集合
        Set<AbstractNode> mustWrapper = new HashSet<>();
        for (AbstractNode dependWrapper : dependWrappers) {
            if (dependWrapper.isMust()) {
                mustWrapper.add(dependWrapper);
            }
            if (dependWrapper.equals(fromWrapper)) {
                nowDependIsMust = dependWrapper.isMust();
            }
        }

        //如果全部是不必须的条件，那么只要到了这里，就执行自己。
        if (mustWrapper.size() == 0) {
            if (ResultState.TIMEOUT == fromWrapper.getWorkResult().getResultState()) {
                fastFail(INIT, null);
            } else {
                fire(context);
            }
            runSonHandler(context,executorService, now, remainTime);
            return;
        }

        //如果存在需要必须完成的，且fromWrapper不是必须的，就什么也不干
        if (!nowDependIsMust) {
            return;
        }

        //如果fromWrapper是必须的
        boolean existNoFinish = false;
        boolean hasError = false;
        //先判断前面必须要执行的依赖任务的执行结果，如果有任何一个失败，那就不用走action了，直接给自己设置为失败，进行下一步就是了
        for (AbstractNode dependWrapper : mustWrapper) {
//            WorkerWrapper workerWrapper = dependWrapper.getDependWrapper();
            WorkResult tempWorkResult = dependWrapper.getWorkResult();
            //为null或者isWorking，说明它依赖的某个任务还没执行到或没执行完
            if (dependWrapper.getState() == INIT || dependWrapper.getState() == WORKING) {
                existNoFinish = true;
                break;
            }
            if (ResultState.TIMEOUT == tempWorkResult.getResultState()) {
                workResult = defaultResult();
                hasError = true;
                break;
            }
            if (ResultState.EXCEPTION == tempWorkResult.getResultState()) {
                workResult = defaultExResult(dependWrapper.getWorkResult().getEx());
                hasError = true;
                break;
            }

        }
        //只要有失败的
        if (hasError) {
            fastFail(INIT, null);
            runSonHandler(context,executorService, now, remainTime);
            return;
        }

        //如果上游都没有失败，分为两种情况，一种是都finish了，一种是有的在working
        //都finish的话
        if (!existNoFinish) {
            //上游都finish了，进行自己
            fire(context);
            runSonHandler(context,executorService, now, remainTime);
            return;
        }
    }
    /**
     * 执行自己的job.具体的执行是在另一个线程里,但判断阻塞超时是在work线程
     */
    private void fire(Context context) {
        //阻塞取结果
        workResult = workerDoJob(context);

    }
    /**
     * 具体的单个worker执行任务
     */
    private WorkResult<V> workerDoJob(Context context) {
        //避免重复执行
        if (!checkIsNullResult()) {
            return workResult;
        }
        V workResults = null;
        try {
            //如果已经不是init状态了，说明正在被执行或已执行完毕。这一步很重要，可以保证任务不被重复执行
            if (!compareAndSetState(INIT, WORKING)) {
                return workResult;
            }
            // 执行回调函数
            begin();
            //执行耗时操作
            // 执行自身逻辑
            workResults = RetryUtil.<V>fromTask(() -> {
                try {
                    V resultValue = this.action(context, param, forParamUserMap);
                    return resultValue;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }).retryTimes(retryTimes).retryTimesLong(5000).getResult().get();
        } catch (Exception e) {
            fastFail(WORKING, e);
        }finally {
            //避免重复回调
            if (!checkIsNullResult()) {
                return workResult;
            }
            //如果状态不是在working,说明别的地方已经修改了
            if (!compareAndSetState(WORKING, FINISH)) {
                return workResult;
            }
            workResult.setResultState(ResultState.SUCCESS);
            workResult.setResult(workResults);
            //回调成功
            result(true, param, workResult);
            return workResult;
        }
    }
    private WorkResult<V> defaultResult() {
        workResult.setResultState(ResultState.TIMEOUT);
        workResult.setResult(this.defaultValue());
        return workResult;
    }
    /**
     * 总控制台超时，停止所有任务
     */
    public void stopNow() {
        if (getState() == INIT || getState() == WORKING) {
            fastFail(getState(), null);
        }
    }
    private boolean compareAndSetState(int expect, int update) {
        return this.state.compareAndSet(expect, update);
    }
    private boolean checkIsNullResult() {
        return ResultState.DEFAULT == workResult.getResultState();
    }
    /**
     * 快速失败
     */
    private boolean fastFail(int expect, Exception e) {
        //试图将它从expect状态,改成Error
        if (!compareAndSetState(expect, ERROR)) {
            return false;
        }
        //尚未处理过结果
        if (checkIsNullResult()) {
            if (e == null) {
                workResult = WorkResult.defaultResult(taskName);
            } else {
                workResult = defaultExResult(e);
            }
        }
        result(false, param, workResult);
        return true;
    }
    // 返回节点状态
    private int getState() {
        return state.get();
    }
    private WorkResult<V> defaultExResult(Exception ex) {
        workResult.setResultState(ResultState.EXCEPTION);
        workResult.setResult(this.defaultValue());
        workResult.setEx(ex);
        return workResult;
    }
    /**
     * 判断自己下游链路上，是否存在已经出结果的或已经开始执行的
     * 如果没有返回true，如果有返回false
     */
    private boolean checkNextWrapperResult() {
        //如果自己就是最后一个，或者后面有并行的多个，就返回true
        if (this.sonHandler == null || this.sonHandler.size() != 1) {
            return getState() == INIT;
        }
        AbstractNode nextWrapper = sonHandler.get(0);
        boolean state = nextWrapper.getState() == INIT;
        //继续校验自己的next的状态
        return state && nextWrapper.checkNextWrapperResult();
    }
    /**
     * 执行当前节点的子节点
     *
     * @param context         上下文
     * @param executorService 线程池
     * @param now             当前时间
     * @param remainTime      距离指定的超时时间还剩余的时间
     */
    public void runSonHandler(Context context, ExecutorService executorService,
                              long now, long remainTime) {
        // 没有子节点的时候
        if (CollectionUtils.isEmpty(this.sonHandler)) {
            return;
        }
        //花费的时间
        long costTime = SystemClock.now() - now;
        // 根据上游进行筛选
        if (choose !=null){
            Iterator<AbstractNode> iterator = sonHandler.iterator();
            while (iterator.hasNext()){
                AbstractNode next = iterator.next();
                if (!next.nodeName().equals(choose)){
                    iterator.remove();
                }
            }
        }
        CompletableFuture[] count = new CompletableFuture[sonHandler.size()];
        for (int i = 0; i < sonHandler.size(); i++) {
            AbstractNode node = sonHandler.get(i);
            count[i] = CompletableFuture.runAsync(() ->
            {
                node.template(executorService, this, remainTime - costTime, forParamUserMap, context);
            }, executorService);

        }
        try {
            CompletableFuture.allOf(count).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public String toString() {
        return "AbstractNode{" +
                "AbstractNode=" + this.nodeName() +
                ", must=" + must +
                '}';
    }
}