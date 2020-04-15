package contract.migration.util;

import contract.migration.exception.ResourceMissingException;
import contract.migration.data.ChildObjects;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class MigrationUtil {
    private Map<String, ChildObjects> contractMaster = new HashMap<>();
    private List<String> leaseHeaders = new ArrayList<>(); private int contractHeaderLease = -1;
    private List<String> equipmentHeaders = new ArrayList<>(); private int contractHeaderEqpmnt = -1;
    private List<String> paymentHeaders = new ArrayList<>(); private int contractHeaderPayment = -1;
    private List<String> feesHeaders = new ArrayList<>(); private int contractHeaderFees = -1;
    private List<String> partiesHeaders = new ArrayList<>(); private int contractHeaderParty = -1;
    private List<String> billsHeaders = new ArrayList<>(); private int contractHeaderBill = -1;
    private Properties props;
    public Map<Integer, JSONObject> run(Properties props) throws JSONException, ResourceMissingException, IOException {
        Map<Integer, JSONObject> migration = new HashMap<>();
        this.props = props;
        if (null != props.get("file.contracts") && props.get("file.contracts").toString().trim().length() > 0) {
            File contracts =
                    new File(props.get("file.contracts").toString());
            if (null != contracts)
                readContractsFile(contracts);
            else
                throw new ResourceMissingException("Contracts file does not exist");
        } else {
            throw new ResourceMissingException("Contracts file does not exist");
        }

        if (null != props.get("file.equipments") && props.get("file.equipments").toString().trim().length() > 0) {
            File equipments =
                    new File(props.get("file.equipments").toString());
            if (null != equipments)
                readEquipmentsFile(equipments);
            else
                throw new ResourceMissingException("Equipments file does not exist");
        } else {
            throw new ResourceMissingException("Equipments file does not exist");
        }

        if (null != props.get("file.payments") && props.get("file.payments").toString().trim().length() > 0) {
            File paymentSchedule =
                    new File(props.get("file.payments").toString());
            if (null != paymentSchedule)
                readPaymentsFile(paymentSchedule);
        }

        if (null != props.get("file.fees") && props.get("file.fees").toString().trim().length() > 0) {
            File fees =
                    new File(props.get("file.fees").toString());
            if (null != fees)
                readFeesFile(fees);
        }

        if (null != props.get("file.parties") && props.get("file.parties").toString().trim().length() > 0) {
            File parties =
                    new File(props.get("file.parties").toString());
            if (null != parties)
                readPartiesFile(parties);
        }

        if (null != props.get("file.bills") && props.get("file.bills").toString().trim().length() > 0) {
            File parties =
                    new File(props.get("file.bills").toString());
            if (null != parties)
                readBillsFile(parties);
        }

        int rowIndex = 0;
        for (String contract : contractMaster.keySet()) {
            System.out.println("Error line " + (rowIndex+1));
            System.out.println("Contract > " + contract + "Body > " + contractMaster.get(contract).toString());


            JSONObject jContract = createContractBody(contract, contractMaster.get(contract));
            if (null == jContract) continue;
            System.out.println(jContract.toString(4));
            migration.put(contractMaster.get(contract).rowNumber, jContract);
            rowIndex++;
        }

        return migration;
    }

    private JSONObject createContractBody(String contract, ChildObjects childObjects) throws JSONException {
        JSONObject mainObj = new JSONObject();
        JSONObject lsContract = new JSONObject();
        JSONObject attributes = new JSONObject();
        attributes.put("type", "cllease__Lease_Account__c");
        lsContract.put("attributes", attributes);

        String[] data = childObjects.contractRow.split(",");
        for (int i = 0; i < leaseHeaders.size(); i++) {
            if (leaseHeaders.get(i).equalsIgnoreCase("ID")
                    || leaseHeaders.get(i).equalsIgnoreCase("contract__c")
                    || leaseHeaders.get(i).equalsIgnoreCase("cllease__contract__c")) {
                continue;
            }
            if (leaseHeaders.get(i).equalsIgnoreCase("cllease__Has_Asset_Level_Rent__c")
                    || (leaseHeaders.get(i).equalsIgnoreCase("Has_Asset_Level_Rent__c"))) {
                if (data[i].equalsIgnoreCase("yes") || data[i].equalsIgnoreCase("true"))
                    childObjects.hasAssetLevelRent = true;
            }
            lsContract.put(leaseHeaders.get(i), data[i]);
            contractMaster.get(contract).contractObject.put(leaseHeaders.get(i), data[i]);
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        //putting the is migrated flag as true
        if (null != this.props.getProperty("mark.migrated.flag") ? Boolean.valueOf(this.props.getProperty("mark.migrated.flag")) : false)
            lsContract.put("isMigrated__c", true);
        lsContract.put("cllease__Migration_Date__c", this.props.getProperty("migration.date", format.format(new Date())));
        mainObj.put("LS Contract", lsContract.toString().replaceAll("\"", "\\\""));
        JSONObject relatedObjects = new JSONObject();
        mainObj.put("relatedObjects", relatedObjects.toString().replaceAll("\"", "\\\""));
        JSONObject childObject = new JSONObject();

        JSONObject childObjectsPrep = createEquipmentArray(contract, childObject);

        if (null != childObjectsPrep) {
            mainObj.put("childObjects", childObjectsPrep.toString().replaceAll("\"", "\\\""));
            mainObj.put("actionParam", "1");
            return mainObj;
        } else {
            return null;
        }
    }

    private void readPartiesFile(File parties) throws FileNotFoundException {
        Scanner scanner = new Scanner(parties);
        int rowIndex = 0;
        while(scanner.hasNextLine()) {
            String row = scanner.nextLine();
            if (rowIndex == 0) {
                //means header
                String[] hdrs = row.split(",");
                int colIndex = 0;
                for (String hdr : hdrs) {
                    String header = hdr.trim().replaceAll("[\\uFEFF-\\uFFFF]", "");
                    if (header.toLowerCase().toLowerCase().equalsIgnoreCase(this.props.getProperty("contract.key"))) {
                        contractHeaderParty = colIndex;
                    }
                    partiesHeaders.add(header);
                    colIndex++;
                }
            } else {
                String[] data = row.split(",");
                if (contractHeaderParty != -1 && null != data[contractHeaderParty] && data[contractHeaderParty].trim().length() > 0) {
                    if (contractMaster.containsKey(data[contractHeaderParty])) {
                        ChildObjects childObjects = contractMaster.get(data[contractHeaderParty]);
                        childObjects.parties.add(row);
                    } else {
                        ChildObjects childObjects = new ChildObjects();
                        childObjects.parties.add(row);
                        contractMaster.put(data[contractHeaderParty], childObjects);
                    }
                }
            }
            rowIndex++;
        }
    }

    private void readBillsFile(File parties) throws FileNotFoundException {
        Scanner scanner = new Scanner(parties);
        int rowIndex = 0;
        while(scanner.hasNextLine()) {
            String row = scanner.nextLine();
            if (rowIndex == 0) {
                //means header
                String[] hdrs = row.split(",");
                int colIndex = 0;
                for (String hdr : hdrs) {
                    String header = hdr.trim().replaceAll("[\\uFEFF-\\uFFFF]", "");
                    if ( header.toLowerCase().toLowerCase().equalsIgnoreCase(this.props.getProperty("contract.key")) ) {
                        contractHeaderBill = colIndex;
                    }
                    billsHeaders.add(header);
                    colIndex++;
                }
            } else {
                String[] data = row.split(",");
                if (contractHeaderBill != -1 && null != data[contractHeaderBill] && data[contractHeaderBill].trim().length() > 0) {
                    if (contractMaster.containsKey(data[contractHeaderBill])) {
                        ChildObjects childObjects = contractMaster.get(data[contractHeaderBill]);
                        childObjects.bills.add(row);
                    } else {
                        ChildObjects childObjects = new ChildObjects();
                        childObjects.bills.add(row);
                        contractMaster.put(data[contractHeaderBill], childObjects);
                    }
                }
            }
            rowIndex++;
        }
    }

    private void readFeesFile(File fees) throws FileNotFoundException {
        Scanner scanner = new Scanner(fees);
        int rowIndex = 0;
        while(scanner.hasNextLine()) {
            String row = scanner.nextLine();
            if (rowIndex == 0) {
                //means header
                String[] hdrs = row.split(",");
                int colIndex = 0;
                for (String hdr : hdrs) {
                    String header = hdr.trim().replaceAll("[\\uFEFF-\\uFFFF]", "").replaceAll("\\s\t\"", "");;
                    if ( header.toLowerCase().equalsIgnoreCase(this.props.getProperty("contract.key")) ) {
                        contractHeaderFees = colIndex;
                    }
                    feesHeaders.add(header);
                    colIndex++;
                }
            } else {
                String[] data = row.split(",");
                if (contractHeaderFees != -1 && null != data[contractHeaderFees] && data[contractHeaderFees].trim().length() > 0) {
                    if (contractMaster.containsKey(data[contractHeaderFees])) {
                        ChildObjects childObjects = contractMaster.get(data[contractHeaderFees]);
                        childObjects.contractFee.add(row);
                    } else {
                        ChildObjects childObjects = new ChildObjects();
                        childObjects.contractFee.add(row);
                        contractMaster.put(data[contractHeaderFees], childObjects);
                    }
                }
            }
            rowIndex++;
        }
    }

    private void readPaymentsFile(File paymentSchedule) throws FileNotFoundException {
        Scanner scanner = new Scanner(paymentSchedule);
        int rowIndex = 0;
        while(scanner.hasNextLine()) {
            String row = scanner.nextLine();
            if (rowIndex == 0) {
                //means header
                String[] hdrs = row.split(",");
                int colIndex = 0;
                for (String hdr : hdrs) {
                    String header = hdr.trim().replaceAll("[\\uFEFF-\\uFFFF]", "").replaceAll("\\s\t\"", "");;
                    if ( header.toLowerCase().equalsIgnoreCase(this.props.getProperty("contract.key")) ) {
                        contractHeaderPayment = colIndex;
                    }
                    paymentHeaders.add(header);
                    colIndex++;
                }
            } else {
                String[] data = row.split(",");
                if (contractHeaderPayment != -1 && null != data[contractHeaderPayment] && data[contractHeaderPayment].trim().length() > 0) {
                    if (contractMaster.containsKey(data[contractHeaderPayment])) {
                        ChildObjects childObjects = contractMaster.get(data[contractHeaderPayment]);
                        childObjects.pmtSchedule.add(row);
                    } else {
                        ChildObjects childObjects = new ChildObjects();
                        childObjects.pmtSchedule.add(row);
                        contractMaster.put(data[contractHeaderPayment], childObjects);
                    }
                }
            }
            rowIndex++;
        }
    }

    private void readEquipmentsFile(File equipments) throws FileNotFoundException {
        Scanner scanner = new Scanner(equipments);
        int rowIndex = 0;
        while(scanner.hasNextLine()) {
            String row = scanner.nextLine();
            if (rowIndex == 0) {
                //means header
                String[] hdrs = row.split(",");
                int colIndex = 0;
                for (String hdr : hdrs) {
                    String header = hdr.trim().replaceAll("[\\uFEFF-\\uFFFF]", "").replaceAll("\\s\t\"", "");
                    if ( header.toLowerCase().equalsIgnoreCase(this.props.getProperty("contract.key")) ) {
                        contractHeaderEqpmnt = colIndex;
                    }
                    equipmentHeaders.add(header);
                    colIndex++;
                }
            } else {
                String[] data = row.split(",");
                if (contractHeaderEqpmnt != -1 && null != data[contractHeaderEqpmnt] && data[contractHeaderEqpmnt].trim().length() > 0) {
                    if (contractMaster.containsKey(data[contractHeaderEqpmnt])) {
                        ChildObjects childObjects = contractMaster.get(data[contractHeaderEqpmnt]);
                        childObjects.equipments.add(row);
                    } else {
                        ChildObjects childObjects = new ChildObjects();
                        childObjects.equipments.add(row);
                        contractMaster.put(data[contractHeaderEqpmnt], childObjects);
                    }
                }
            }
            rowIndex++;
        }
    }

    private void readContractsFile(File contracts) throws FileNotFoundException {
        Scanner scanner = new Scanner(contracts);
        int rowIndex = 0;
        while(scanner.hasNextLine()) {
            String row = scanner.nextLine();
            if (rowIndex == 0) {
                //means header
                String[] hdrs = row.split(",");
                int colIndex = 0;
                for (String hdr : hdrs) {
                    String header = hdr.trim().replaceAll("[\\uFEFF-\\uFFFF]", "").replaceAll("\\s\t\"", "");
                    if ( header.toLowerCase().equalsIgnoreCase(this.props.getProperty("contract.key")) ) {
                        contractHeaderLease = colIndex;
                    }
                    leaseHeaders.add(header);
                    colIndex++;
                }
            } else {
                String[] data = row.split(",");
                if (contractHeaderLease != -1 && null != data[contractHeaderLease] && data[contractHeaderLease].trim().length() > 0) {
                    if (contractMaster.containsKey(data[contractHeaderLease])) {
                        ChildObjects childObjects = contractMaster.get(data[contractHeaderLease]);
                        childObjects.contractRow = row;
                        childObjects.rowNumber = rowIndex;
                    } else {
                        ChildObjects childObjects = new ChildObjects();
                        childObjects.contractRow = row;
                        childObjects.rowNumber = rowIndex;
                        contractMaster.put(data[contractHeaderLease], childObjects);
                    }
                }
            }
            rowIndex++;
        }
    }

    private JSONObject createEquipmentArray(String contract, JSONObject childObjects) throws JSONException {
        JSONArray equipArray = createJSONArray(contractMaster.get(contract).equipments, equipmentHeaders);
        if (null != equipArray && equipArray.length() > 0) {
//            childObjects.put("Contract_Equipment__c", equipArray);
            if (null != this.props.getProperty("mark.migrated.flag") ? Boolean.valueOf(this.props.getProperty("mark.migrated.flag")) : false) {
                for (int i = 0; i < equipArray.length(); i++) {
                    equipArray.getJSONObject(i).put("isMigrated__c", true);
                }
            }

            childObjects.put("cllease__Contract_Equipment__c".toLowerCase(), equipArray);
            return createPartiesArray(contract, childObjects);
        } else {
           //contractMaster.remove(contract);
           return null;
        }
    }

    private JSONObject createPartiesArray(String contract, JSONObject childObjects) throws JSONException {
        JSONArray partArray = createJSONArray(contractMaster.get(contract).parties, partiesHeaders);
        if (null != partArray && partArray.length() > 0)
            childObjects.put("cllease__Contract_Parties__c".toLowerCase(), partArray);
        return createPaymentsArray(contract, childObjects);
    }

    private JSONObject createPaymentsArray(String contract, JSONObject childObjects) throws JSONException {
        if (contractMaster.get(contract).hasAssetLevelRent) {
            //create a single payment schedule as per rules Rent__c on all equipments
            JSONArray equipArr = childObjects.getJSONArray("cllease__Contract_Equipment__c".toLowerCase());
            double rent = 0.0;
            for (int i = 0; i < equipArr.length(); i++) {
                JSONObject equipment = equipArr.getJSONObject(i);
                if (null != equipment.get("cllease__Rent__c")
                        && equipment.getString("cllease__Rent__c").trim().length() > 0
                        && equipment.getString("cllease__Rent__c").matches("\\d+(.\\d)*|\\d*\\.\\d+|\\d+\\.\\d*")) {
                    rent += Double.valueOf(equipment.getString("cllease__Rent__c"));
                }
            }
            JSONArray payArray = new JSONArray();
            JSONObject payment = new JSONObject();

            payment.put("cllease__Payment_Amount__c", String.valueOf(rent));
            payment.put("cllease__Total_Payment__c", String.valueOf(rent));
            payment.put("cllease__Number_Of_Payments__c", contractMaster.get(contract).contractObject.getString("cllease__Term__c"));
            payment.put("cllease__Frequency__c", contractMaster.get(contract).contractObject.getString("cllease__Payment_Frequency__c"));
            payment.put("cllease__Payment_Date__c", contractMaster.get(contract).contractObject.getString("cllease__First_Payment_Date__c"));
            payment.put("cllease__Sequence__c", "1");
            payment.put("cllease__VAT__c", "0");


            payArray.put(payment);
            childObjects.put("cllease__Payment_Schedule__c".toLowerCase(), payArray);
        } else {
            JSONArray pmtArray = createJSONArray(contractMaster.get(contract).pmtSchedule, paymentHeaders);
            if (null != pmtArray && pmtArray.length() > 0)
                childObjects.put("cllease__Payment_Schedule__c".toLowerCase(), pmtArray);
        }
        return createFeesArray(contract, childObjects);
    }

    private JSONObject createFeesArray(String contract, JSONObject childObjects) throws JSONException {
        JSONArray feeArray = createJSONArray(contractMaster.get(contract).contractFee, feesHeaders);
        if (null != feeArray && feeArray.length() > 0) {
            if (null != this.props.getProperty("mark.migrated.flag") ? Boolean.valueOf(this.props.getProperty("mark.migrated.flag")) : false) {
                for (int i = 0; i < feeArray.length(); i++) {
                    feeArray.getJSONObject(i).put("isMigrated__c", true);
                }
            }
            childObjects.put("cllease__Contract_Fees__c".toLowerCase(), feeArray);
        }
        return createBillsArray(contract, childObjects);
    }

    private JSONObject createBillsArray(String contract, JSONObject childObjects) throws JSONException {
        JSONArray billArray = createJSONArray(contractMaster.get(contract).bills, billsHeaders);
        if (null != billArray && billArray.length() > 0) {
            JSONObject migratedFlag = new JSONObject();
            if (null != this.props.getProperty("mark.migrated.flag") ? Boolean.valueOf(this.props.getProperty("mark.migrated.flag")) : false)
                migratedFlag.put("isMigrated__c", true);
            billArray.put(migratedFlag);
            childObjects.put("cllease__Lease_account_Due_Details__c".toLowerCase(), billArray);
        }
        return childObjects;
    }



    private JSONArray createJSONArray(List<String> data, List<String> header) throws JSONException {
        JSONArray arr = new JSONArray();
        for (String row : data) {
            JSONObject attr = new JSONObject();
            String[] rowdata = row.split(",");
            for (int i = 0; i < header.size(); i++) {
                if (header.get(i).toLowerCase().equalsIgnoreCase(this.props.getProperty("contract.key"))) {
                    continue;
                }
                if (rowdata[i].trim().replaceAll("[\\uFEFF-\\uFFFF]", "").replaceAll("\\s\t\"", "").replaceAll(" ", "").length() > 0)
                    attr.put(header.get(i), rowdata[i].trim().replaceAll("[\\uFEFF-\\uFFFF]", "").replaceAll("\\s\t\"", "").replaceAll(" ", ""));
            }
            arr.put(attr);
        }
        return arr;
    }
}

