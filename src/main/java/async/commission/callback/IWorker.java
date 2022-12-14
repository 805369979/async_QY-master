package async.commission.callback;

import async.commission.entity.Context;
import async.commission.template.AbstractNode;

import java.util.Map;

/**
 * 每个最小执行单元需要实现该接口
 */
@FunctionalInterface
public interface IWorker<T,V> {
    /**
     * 在这里做耗时操作，如rpc请求、IO等
     */
    V action(Context context,T param, Map<String, AbstractNode<T,V>> allWrappers) throws InterruptedException;

    /**
     * 超时、异常时，返回的默认值
     */
    default V defaultValue() {
        return null;
    }
}
