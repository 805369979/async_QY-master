package async.commission.demo.demo1;

import async.commission.entity.Context;
import async.commission.entity.WorkResult;
import async.commission.executor.timer.SystemClock;
import async.commission.template.AbstractNode;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;

/**
 * 秦同学
 */
@Component
public class Node8 extends AbstractNode<String,String> {

    @Override
    public String action(Context context,String param, Map<String, AbstractNode<String,String>> nodes) throws InterruptedException {
        return this.getTaskName() + "执行完成";
    }

    /**
     * 任务开始的监听
     */
    @Override
    public void begin() {
        super.begin();
        System.out.println(taskName + "开始执行");
    }

    @Override
    public void result(boolean success, String param, WorkResult<String> workResult) {
        if (success) {
            System.out.println(taskName+" 运行完成 callback success--" + SystemClock.now() + "----" + workResult.getResult()
                    + "-threadName:" + Thread.currentThread().getName());
        } else {
            System.err.println(taskName+" 运行完成 callback failure--"+this.getTaskName()+ "----"+ SystemClock.now() + "----" + workResult.getResult()
                    + "-threadName:" + Thread.currentThread().getName());
        }
    }

    @Override
    public String defaultValue() {
        return "worker0--default";
    }
}
