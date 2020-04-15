package contract.migration.exception;

public class ResourceMissingException extends Throwable {
    public ResourceMissingException(String message) {
        super(message);
    }
}
