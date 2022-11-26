package async.commission.util;
import java.util.Optional;
import java.util.function.Supplier;

/**
 *
 * @param <R> 定义重试任务的返回结果类型是什么
 */
public class RetryUtil<R> {
    //重试等待时间长度
    private long retryTimesLong = 500L;
    //重试次数
    private int retryTimes = 3;
    // 返回结果
    private R result;
    // 需要重试的业务逻辑
    private Supplier<R> supplier;

    // 重试业务入口处
    public static <T> RetryUtil <T> fromTask(Supplier<T> supplier) {
        // 重试任务实力定义
        RetryUtil<T> retryUtil = new RetryUtil<>();
        retryUtil.setSupplier(supplier);
        return retryUtil;
    }

    public RetryUtil<R> setSupplier(Supplier<R> supplier) {
        this.supplier = supplier;
        return this;
    }

    public RetryUtil<R> retryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
        return this;
    }

    public RetryUtil<R> retryTimesLong(long retryTimesLong) {
        this.retryTimesLong = retryTimesLong;
        return this;
    }

    // 重试业务逻辑
    public Optional<R> getResult() {
        int curRetryTime = 1;
        // 辅助判断是否重试成功
        boolean retryIsSuccess = Boolean.FALSE;
        while (curRetryTime < retryTimes + 1) {
            try {
                // 执行逻辑
                result = this.supplier.get();
                retryIsSuccess = Boolean.TRUE;
            } catch (Exception e) {
                if ((!retryIsSuccess) && curRetryTime < this.retryTimes) {
                    // 进行下一轮重试
                    try {
                        curRetryTime++;
                        Thread.sleep(this.retryTimesLong);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    continue;
                }
                // 异常且无需重试
                throw new RuntimeException(e);
            }
            // 检查是否运行成功
            if (retryIsSuccess) {
                // 运行成功，返回结果
                return (Optional<R>) Optional.ofNullable(result);
            }
        }
        // 运行到这里说明一次都没有成功
        return Optional.empty();
    }
}
