package contract.migration.pipe;

import contract.migration.data.UploadData;

import java.util.LinkedList;

public class UploadPipeline implements PipeLine<UploadData> {
    private LinkedList<UploadData> pipe;

    public UploadPipeline() {
        this.pipe = new LinkedList<>();
    }

    @Override
    public synchronized void push(UploadData uploadData) {
        this.pipe.add(uploadData);
    }

    @Override
    public synchronized void pushRetry(UploadData uploadData) {
        this.pipe.addFirst(uploadData);
    }

    @Override
    public synchronized UploadData pop() {
        if (this.pipe.size() == 0) return null;
        UploadData packet = this.pipe.get(this.pipe.size() - 1);
        if (packet.retries == 0) {
            this.pipe.remove(this.pipe.size() - 1);
            return new UploadData.Builder(null, -1).build();
        }
        packet.retries -= 1;
        this.pipe.remove(this.pipe.size() - 1);
        return packet;
    }

    @Override
    public synchronized int size() {
        return this.pipe.size();
    }

    @Override
    public String toString() {
        return "UploadPipeline{" +
                "pipe=" + pipe +
                '}';
    }
}
