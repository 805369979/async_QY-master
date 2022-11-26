import async.MainApplication;
import async.commission.demo.demo1.MissionProcess1;
import async.commission.demo.demo2.MissionProcess2;
import async.commission.demo.demo3.MissionProcess3;
import async.commission.demo.demo4.MissionProcess4;
import async.commission.util.SpringUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.ExecutionException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = MainApplication.class)
public class CommissionTest {

    @Test
    public void testDemo1() throws ExecutionException, InterruptedException {
        MissionProcess1 missionProcess = (MissionProcess1) SpringUtil.getBean("MissionProcess1");
        missionProcess.run();
    }
    @Test
    public void testDemo2() throws ExecutionException, InterruptedException {
        MissionProcess2 missionProcess = (MissionProcess2) SpringUtil.getBean("MissionProcess2");
        missionProcess.run();
    }
    @Test
    public void testDemo3() throws ExecutionException, InterruptedException {
        MissionProcess3 missionProcess = (MissionProcess3) SpringUtil.getBean("MissionProcess3");
        missionProcess.run();
    }
    @Test
    public void testDemo4() throws ExecutionException, InterruptedException {
        MissionProcess4 missionProcess = (MissionProcess4) SpringUtil.getBean("MissionProcess4");
        missionProcess.run();
    }
}
