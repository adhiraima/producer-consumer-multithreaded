package contract.migration.data;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class ChildObjects {
    public int rowNumber = -1;
    public String contractRow = new String();
    public List<String> equipments = new ArrayList<>();
    public List<String> parties = new ArrayList<>();
    public List<String> pmtSchedule = new ArrayList<>();
    public List<String> contractFee = new ArrayList<>();
    public List<String> bills = new ArrayList<>();
    public boolean hasAssetLevelRent = false;
    public JSONObject contractObject = new JSONObject();
}
