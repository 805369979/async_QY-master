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
    /**
     * 是否是必须执行该节点,其他支路执行完毕，则该节点不需要执行
     * A----
     *      ----C
     * B----
     * A为false，那么B执行完成后，若A还没执行，则A可不需要执行了
     */
    @Getter
    public boolean must = true;
    /**
     * 节点的名字 默认使用类名
     */
    @Getter
    protected String taskName = this.getClass().getSimpleName();
    /**
     * 节点执行结果状态
     */
    @Getter
    ResultState status = ResultState.DEFAULT;
    /**
     * 将来要处理的任务节点特有的param参数
     */
    @Getter
    protected T param;
    /**
     * 是否需要根据当前计算节点指定选择后续的执行节点
     */
    @Getter
    protected String choose;
    /**
     * 重试次数
     */
    @Getter
    protected int retryTimes = 3;
    /**
     * 重试时间间隔
     */
    @Getter
    protected int retryTimesLong = 5000;
    /**
     * 是否在执行自己前，去校验nextWrapper的执行结果<p>
     * 1   4
     * --------3
     * 2
     * 如这种在4执行前，可能3已经执行完毕了（被2执行完后触发的），那么4就没必要执行了。
     * 注意，该属性仅在nextWrapper数量<=1时有效，>1时的情况是不存在的
     */
    @Getter
    protected volatile boolean needCheckNextNodeResult = true;
    /**
     * 节点状态
     */
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
    private final AtomicInteger state = new AtomicInteger(0);
    /**
     * 默认节点执行结果
     */
    @Getter
    @Setter
    private volatile WorkResult<V> workResult = WorkResult.defaultResult(getTaskName());
    /**
     * 该map存放所有的节点名字和节点的映射
     */
    @Getter
    private ConcurrentHashMap<String, AbstractNode<T,V>> forParamUserMap;
    /**
     * 依赖的父节点
     */
    @Getter
    protected List<AbstractNode> fatherHandler = new ArrayList<>();
    /**
     * 下游的子节点
     */
    @Getter
    protected List<AbstractNode> sonHandler = new ArrayList<>();
    /**
     * 修改taskName
     */
    public AbstractNode<T,V> setTaskNodeName(String taskName){
        this.taskName = taskName;
        return this;
    }

    public AbstractNode<T,V> setParam(T param) {
        this.param = param;
        return this;
    }

    public AbstractNode<T,V> setChoose(String choose) {
        this.choose = choose;
        return this;
    }

    public AbstractNode<T,V> setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
        return this;
    }

    public AbstractNode<T,V> serRetryTimesLong(int retryTimesLong) {
        this.retryTimesLong = retryTimesLong;
        return this;
    }

    public AbstractNode<T,V> setNeedCheckNextNodeResult(boolean needCheckNextNodeResult) {
        this.needCheckNextNodeResult = needCheckNextNodeResult;
        return this;
    }

    public AbstractNode<T,V> setMust(boolean must) {
        this.must = must;
        return this;
    }

    protected void setTaskName(String taskName){
        this.taskName = taskName;
    }

    public AbstractNode<T, V> setSonHandler(List<AbstractNode> nodes){
        this.sonHandler = nodes;
        return this;
    }
    public AbstractNode<T, V> setFatherHandler(List<AbstractNode> nodes){
        this.fatherHandler = nodes;
        return this;
    }
    public AbstractNode<T, V> setSonHandler(AbstractNode ...nodes){
        ArrayList<AbstractNode> nodeList = new ArrayList<>();
        for (AbstractNode node : nodes){
            //adding to list
            nodeList.add(node);
        }
        return setSonHandler(nodeList);
    }
    public void setFatherHandler(AbstractNode ...nodes){
        ArrayList<AbstractNode> nodeList = new ArrayList<>();
        for (AbstractNode node : nodes){
            //adding to list
            nodeList.add(node);
        }
        setFatherHandler(nodeList);
    }


    /**
     * 任务入口函数
     * @param executorService  线程池
     * @param fromNode  来源于哪个父节点
     * @param remainTime  执行该节点剩余的时间
     * @param forParamUseNodes  全局所有的节点
     * @param context   上下文
     */
    public void template(ExecutorService executorService, AbstractNode fromNode, long remainTime,
                         ConcurrentHashMap<String, AbstractNode<T,V>> forParamUseNodes,
                         Context context) {
        // 1、将注册自己进全局map中
        register(forParamUseNodes);
        long now = SystemClock.now();
        // 2、执行任务
        // 2.1 没有剩余执行时间，则快速失败
        if (remainTime <= 0) {
            fastFail(INIT, null);
            runSonHandler(context,executorService, remainTime, now);
            return;
        }
        //2.2 节点是否执行过或者出错
        //如果自己已经执行过了。
        //可能有多个依赖，其中的一个依赖已经执行完了，并且自己也已开始执行或执行完毕。当另一个依赖执行完毕，又进来该方法时，就不重复处理了
        if (getState() == FINISH || getState() == ERROR) {
            runSonHandler(context,executorService, remainTime, now);
            return;
        }
        // 2.3 后续链节点是否执行，若有则不需要再执行自己
        //如果在执行前需要校验nextWrapper的状态
        if (needCheckNextNodeResult) {
            //如果自己的next链上有已经出结果或已经开始执行的任务了，自己就不用继续了
            if (!checkNextNodeResult()) {
                fastFail(INIT, new SkippedException());
                runSonHandler(context,executorService, remainTime, now);
                return;
            }
        }

        // 2.4 父节点情况判断
        //如果没有任何依赖，说明自己就是第一批要执行的
        if (fatherHandler == null || fatherHandler.size() == 0) {
            runSelf(context);
            runSonHandler(context,executorService, remainTime, now);
            return;
        }

        // 3、执行自己，分情况讨论
        /*如果有前方依赖，存在两种情况
         一种是前面只有一个wrapper。即 A  ->  B
        一种是前面有多个wrapper。A C D ->   B。需要A、C、D都完成了才能轮到B。但是无论是A执行完，还是C执行完，都会去唤醒B。
        所以需要B来做判断，必须A、C、D都完成，自己才能执行 */
        //只有一个依赖
        if (fatherHandler.size() == 1) {
            doDependsOneJob(fromNode,context);
            runSonHandler(context,executorService, remainTime, now);
        } else {
            //有多个依赖时
            doDependsJobs(executorService, fromNode, fatherHandler, now, remainTime,context);
        }
    }

    /**
     * 将自己注册进全局map中
     * @param forParamUserMap 保存全局节点的map
     */
    public void register(ConcurrentHashMap<String, AbstractNode<T,V>> forParamUserMap) {
        this.setTaskName(this.taskName);
        this.forParamUserMap = forParamUserMap;
        forParamUserMap.put(this.taskName, this);
    }

    private void doDependsOneJob(AbstractNode dependWrapper,Context context) {
        if (ResultState.TIMEOUT == dependWrapper.getWorkResult().getResultState()) {
            workResult = timeoutResult();
            fastFail(INIT, null);
        } else if (ResultState.EXCEPTION == dependWrapper.getWorkResult().getResultState()) {
            workResult = defaultExResult(dependWrapper.getWorkResult().getEx());
            fastFail(INIT, null);
        } else {
            //前面任务正常完毕了，该自己了
            runSelf(context);
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
                runSelf(context);
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
                workResult = timeoutResult();
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
            runSelf(context);
            runSonHandler(context,executorService, now, remainTime);
            return;
        }
    }
    /**
     * 执行自己的job.具体的执行是在另一个线程里,但判断阻塞超时是在work线程
     */
    private void runSelf(Context context) {
        //阻塞取结果
        workResult = workerDoJob(context);

    }
    /**
     * 具体的单个worker执行任务
     */
    private WorkResult<V> workerDoJob(Context context) {
        //不是默认状态，则说明执行过，避免重复执行
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
            }).retryTimes(retryTimes).retryTimesLong(retryTimesLong).getResult().get();
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
    private WorkResult<V> timeoutResult() {
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
    private boolean checkNextNodeResult() {
        //如果自己就是最后一个，或者后面有并行的多个，就返回true
        if (this.sonHandler == null || this.sonHandler.size() != 1) {
            return getState() == INIT;
        }
        AbstractNode nextNode = sonHandler.get(0);
        boolean state = nextNode.getState() == INIT;
        //继续校验自己的next的状态
        return state && nextNode.checkNextNodeResult();
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
        // 根据上游指定的节点选择子节点执行
        if (choose !=null){
            Iterator<AbstractNode> iterator = sonHandler.iterator();
            while (iterator.hasNext()){
                AbstractNode next = iterator.next();
                if (!next.getTaskName().equals(choose)){
                    iterator.remove();
                }
            }
        }
        CompletableFuture[] count = new CompletableFuture[sonHandler.size()];
        for (int i = 0; i < sonHandler.size(); i++) {
            AbstractNode node = sonHandler.get(i);
            count[i] = CompletableFuture.runAsync(() -> {
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
                "AbstractNode=" + this.getTaskName() +
                ", must=" + must +
                ", taskName=" + taskName +
                '}';
    }
}