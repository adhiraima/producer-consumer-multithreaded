package contract.migration.data;

import org.json.JSONObject;

public class UploadData {
    public boolean isCreated = false;
    public boolean streamsGenerated = false;
    public boolean isActivated = false;
    public boolean processed = false;
    public JSONObject contract;
    public Integer row;
    public String contractId;
    public int retries;
    public String token;

    public static class Builder {
        public Builder(JSONObject contract, Integer row) {
            this.contract = contract;
            this.row = row;
        }

        private final JSONObject contract;
        private final Integer row;

        private boolean isCreated = false;
        private boolean streamsGenerated = false;
        private boolean isActivated = false;
        private boolean processed = false;
        private String contractId = null;
        private int retries = 0;
        private String token = null;

        public Builder setCreated(Boolean created) { this.isCreated = created; return this; }
        public Builder setGenerated(Boolean generated) { this.streamsGenerated = generated; return this; }
        public Builder setActivated(Boolean activated) { this.isActivated = activated; return this; }
        public Builder processed(Boolean processed) { this.processed = processed; return this; }
        public Builder setContractId(String contractId) { this.contractId = contractId; return this; }
        public Builder setRetires(int retires) { this.retries = retires; return this; }
        public Builder setToken(String token) { this.token = token; return this; }
        public UploadData build() {
            return new UploadData(this);
        }
    }

    private UploadData(Builder builder) {
        isCreated = builder.isCreated;
        streamsGenerated = builder.streamsGenerated;
        isActivated = builder.isActivated;
        processed = builder.processed;
        contract = builder.contract;
        row = builder.row;
        contractId = builder.contractId;
        retries = builder.retries;
        token = builder.token;
        
    }

    @Override
    public String toString() {
        return "UploadData{" +
                "isCreated=" + isCreated +
                ", streamsGenerated=" + streamsGenerated +
                ", isActivated=" + isActivated +
                ", processed=" + processed +
                ", contract=" + contract +
                ", row=" + row +
                ", contractId='" + contractId + '\'' +
                ", retries=" + retries +
                ", token='" + token + '\'' +
                '}';
    }
}
