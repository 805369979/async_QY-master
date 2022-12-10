package async.commission.exception;

/**
 * 如果任务在执行发生异常，则抛该exception
 */
public class SkippedException extends RuntimeException {
    public SkippedException() {
        super();
    }

    public SkippedException(String message) {
        super(message);
    }
}
