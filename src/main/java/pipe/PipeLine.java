package contract.migration.pipe;

public interface PipeLine<T> {
    public void push(T t);
    public void pushRetry(T t);
    public T pop();
    public int size();
}
