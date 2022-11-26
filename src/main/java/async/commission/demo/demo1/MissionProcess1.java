package async.commission.demo.demo1;

import async.commission.Async;
import async.commission.entity.Context;
import async.commission.executor.timer.SystemClock;
import async.commission.template.AbstractNode;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@Component("MissionProcess1")
public class MissionProcess1 {

    public void run() throws InterruptedException, ExecutionException {
        Node1 node1 = new Node1();
        Node2 node2 = new Node2();
        Node3 node3 = new Node3();
        Node4 node4 = new Node4();
        Node5 node5 = new Node5();
        node5.setMust(false);
        Node6 node6 = new Node6();
        Node7 node7 = new Node7();
        Node8 node8 = new Node8();

        // 编排任务
        node1.setSonHandler(node2, node3);

        node2.setFatherHandler(node1);
        node2.setSonHandler(node4, node5);

        node3.setFatherHandler(node1);
        node3.setSonHandler(node6);

        node4.setFatherHandler(node2);
        node4.setSonHandler(node7);

        node5.setFatherHandler(node2);
        node5.setSonHandler(node7);

        node6.setFatherHandler(node3);
        node6.setSonHandler(node7);


        node7.setFatherHandler(node4, node5, node6);
        node7.setSonHandler(node8);

        node8.setFatherHandler(node7);

        // 设置上下文
        Context context1 = new Context();

        long now = SystemClock.now();
        System.out.println("begin-" + now);

        // 开始执行任务
        Map<String, AbstractNode> results = Async.beginWork(100000L, context1, node1);
//        results.forEach((k,v)->{
//            System.out.println(v.getWorkResult());
//        });
        System.out.println("end-" + SystemClock.now());
        System.err.println("cost-" + (SystemClock.now() - now));
        Async.shutDown();
    }
}
