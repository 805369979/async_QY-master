package async.commission.demo.demo1;

import async.commission.Async;
import async.commission.entity.Context;
import async.commission.executor.timer.SystemClock;
import async.commission.template.AbstractNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@Component("MissionProcess1")
public class MissionProcess1 {

    @Autowired
    Node1 node1;
    @Autowired
    Node2 node2;
    @Autowired
    Node3 node3;
    @Autowired
    Node4 node4;
    @Autowired
    Node5 node5;
    @Autowired
    Node6 node6;
    @Autowired
    Node7 node7;
    @Autowired
    Node8 node8;
    public void run() throws InterruptedException, ExecutionException {
        node1.setSonHandler(node2,node3).setMust(true);

        node2.setSonHandler(node4,node5).setFatherHandler(node1);

        node3.setSonHandler(node6).setFatherHandler(node1);

        node4.setSonHandler(node7).setFatherHandler(node2);

        node5.setSonHandler(node7).setFatherHandler(node2);

        node6.setSonHandler(node7).setFatherHandler(node3);

        node7.setSonHandler(node8).setFatherHandler(node4,node5,node6);

        node8.setFatherHandler(node7);

        // 设置上下文
        Context context1 = new Context();

        long now = SystemClock.now();
        System.out.println("begin-" + now);

        // 开始执行任务
        Map<String, AbstractNode> results = Async.startWork(100000L, context1, node1);
//        results.forEach((k,v)->{
//            System.out.println(v.getWorkResult());
//        });
        System.out.println("end-" + SystemClock.now());
        System.err.println("cost-" + (SystemClock.now() - now));
        Async.shutDown();
    }
}
