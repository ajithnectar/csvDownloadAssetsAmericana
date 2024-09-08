package com.ajith.assetCSVdownload.serviceImpl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.ajith.assetCSVdownload.service.FileDownloadService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class FileDownloadServiceImpl implements FileDownloadService {

    @Autowired
    private RestTemplate restTemplate;

    private static final String GRAPHQL_URL = "https://americana.nectarit.com:444/api/graphql";
    private static final String TOKEN = "82bd5653-59ac-39e1-8dc8-b51003630da4";

    // Method to fetch asset list from GraphQL API
    public JsonNode fetchAssetList() throws IOException {
        String query = "query getAssetList($filter: AssetFilter!) {\r\n"
                + "  getAssetList(filter: $filter) {\r\n"
                + "    assets {\r\n"
                + "      category\r\n"
                + "      clientDomain\r\n"
                + "      clientName\r\n"
                + "      communicationStatus\r\n"
                + "      createdOn\r\n"
                + "      criticalAlarm\r\n"
                + "      dataTime\r\n"
                + "      displayName\r\n"
                + "		 underMaintenance\r\n"
                + "      documentExpire\r\n"
                + "      documentExpiryTypes\r\n"
                + "      domain\r\n"
                + "      id\r\n"
                + "      identifier\r\n"
                + "      location\r\n"
                + "      make\r\n"
                + "      type\r\n"
                + "      typeName\r\n"
                + "		 points\r\n"
                + "      model\r\n"
                + "      name\r\n"
                + "      operationStatus\r\n"
                + "      serialNumber\r\n"
                + "		 status\r\n"
                + "    }\r\n"
                + "    totalAssetsCount\r\n"
                + "  }\r\n"
                + "}";

        Map<String, Object> filter = Map.of(
                "offset", 1,
                "pageSize", 20,
                "communicationStatus","COMMUNICATING",
                "order", "asc",
                "searchLabel", "",
                "domain", "nectarfm",
                "type", "DeliveryVan"
        );

        Map<String, Object> requestBody = Map.of(
                "query", query,
                "variables", Map.of("filter", filter)
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(TOKEN);

        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(GRAPHQL_URL, requestEntity, String.class);

        if (response.getStatusCode() != HttpStatus.OK) {
            throw new RuntimeException("Failed to fetch asset data");
        }
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readTree(response.getBody()).path("data").path("getAssetList").path("assets");
    }

    // Method to generate CSV from asset list
    public byte[] generateCsv(JsonNode assetsNode) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(byteArrayOutputStream, StandardCharsets.UTF_8);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ZoneId targetZone = ZoneId.of("Asia/Kolkata");
        
        // Write CSV header
        writer.write("Category,Client Domain,Client Name,Communication Status,Created On,Critical Alarm,Data Time,Display Name,Document Expire,Domain,ID,Identifier,Location,Make,Model,Name,Operation Status,Serial Number\n");

        // Write CSV rows
        for (JsonNode asset : assetsNode) {
            writer.write(asset.path("category").asText() + ",");
            writer.write(asset.path("clientDomain").asText() + ",");
            writer.write(asset.path("clientName").asText() + ",");
            writer.write(asset.path("communicationStatus").asText() + ",");
            String createdOn = convertToTimezone(asset.path("createdOn").asText(), targetZone, formatter);
            writer.write(createdOn + ",");
            writer.write(asset.path("criticalAlarm").asText() + ",");
            String dataTime = convertToTimezone(asset.path("dataTime").asText(), targetZone, formatter);
            writer.write(dataTime + ",");
            writer.write(asset.path("displayName").asText() + ",");
            writer.write(asset.path("documentExpire").asText() + ",");
            writer.write(asset.path("domain").asText() + ",");
            writer.write(asset.path("id").asText() + ",");
            writer.write(asset.path("identifier").asText() + ",");
            writer.write(asset.path("location").asText() + ",");
            writer.write(asset.path("make").asText() + ",");
            writer.write(asset.path("model").asText() + ",");
            writer.write(asset.path("name").asText() + ",");
            writer.write(asset.path("operationStatus").asText() + ",");
            writer.write(asset.path("serialNumber").asText() + "\n");
        }

        writer.flush();
        writer.close();

        return byteArrayOutputStream.toByteArray();
    }
    
    private String convertToTimezone(String dateTimeStr, ZoneId targetZone, DateTimeFormatter formatter) {
        long timestamp = Long.parseLong(dateTimeStr);
        Instant instant = Instant.ofEpochMilli(timestamp);
        ZonedDateTime zonedDateTime = instant.atZone(targetZone);
        return formatter.format(zonedDateTime);
    }

	@Override
	public JsonNode fetchAssetHistoryList(JsonNode asset) throws Exception {
		
		  String query = "query getAssetHistory($data: AssetHistoryInput!){getAssetHistory(data: $data)}";

		  Map<String, Object> dataMap = new HashMap<>();
	        dataMap.put("endDate", 1725733799999L);
	        dataMap.put("startDate", 1725561000000L);
	        
	        // Create the asset data map
	        Map<String, Object> assetDataMap = new HashMap<>();
	        assetDataMap.put("assetCode", null);
	        assetDataMap.put("createdOn", asset.path("createdOn"));
	        assetDataMap.put("dddLink", null);
	        assetDataMap.put("displayName", asset.path("displayName").asText());
	        assetDataMap.put("domain", asset.path("domain").asText());
	        assetDataMap.put("identifier", asset.path("identifier").asText());
	        assetDataMap.put("make", asset.path("make").asText());
	        assetDataMap.put("model",asset.path("model").asText());
	        assetDataMap.put("name", asset.path("name").asText());
	        assetDataMap.put("ownerClientId", "nectarfm");
	        assetDataMap.put("powerRating", null);
	        assetDataMap.put("profileImage", "");
	        assetDataMap.put("sourceTagPath", null);
	        assetDataMap.put("status", "ACTIVE");
	        assetDataMap.put("typeName", asset.path("typeName").asText());

	        // Create the asset map
	        Map<String, Object> assetMap = new HashMap<>();
	        assetMap.put("data", assetDataMap);
	        assetMap.put("type", asset.path("type").asText());
	        
	        // Create the sources list
	        Set<String> uniquePointNames = new HashSet<>();
	        JsonNode points = asset.path("points");
	        for (JsonNode point : points) {
	            uniquePointNames.add(point.path("pointName").asText());
	        }
	        
	        List<Map<String, Object>> sourcesList = Collections.singletonList(
	            new HashMap<String, Object>() {{
	                put("asset", assetMap);
	                put("pointNames", new ArrayList<>(uniquePointNames));
	            }}
	        );

	        // Add the sources list to the data map
	        dataMap.put("sources", sourcesList);

	        // Create the final payload map
	        Map<String, Object> payload = Collections.singletonMap("data", dataMap);

	        // Example usage of the payload variable
	        System.out.println(payload);


	        Map<String, Object> requestBody = Map.of(
	                "query", query,
	                "variables", payload
	        );

	        HttpHeaders headers = new HttpHeaders();
	        headers.setContentType(MediaType.APPLICATION_JSON);
	        headers.setBearerAuth(TOKEN);
	        
	        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);

	        ResponseEntity<String> response = restTemplate.postForEntity(GRAPHQL_URL, requestEntity, String.class);

	        if (response.getStatusCode() != HttpStatus.OK) {
	            throw new RuntimeException("Failed to fetch asset data");
	        }
	        ObjectMapper objectMapper = new ObjectMapper();
	        JsonNode rootNode = objectMapper.readTree(response.getBody());
	        JsonNode dataNode = rootNode.path("data");
	        JsonNode assetHistoryArray = dataNode.path("getAssetHistory");
	        return assetHistoryArray;
	}

	@Override
	public void generateForEachAssetsCsv(JsonNode asset, JsonNode assetHistory, String baseFolderPath) throws IOException {
	    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	    ZoneId targetZone = ZoneId.of("Asia/Kolkata");

	    String assetId = asset.path("id").asText();
	    String assetFolderPath = baseFolderPath + File.separator + assetId;

	    File assetFolder = new File(assetFolderPath);
	    if (!assetFolder.exists()) {
	        assetFolder.mkdirs();
	    }

	    Map<String, Map<String, Map<String, String>>> monthlyDataMap = new HashMap<>();
	    Set<String> pointNames = new HashSet<>();
	    List<Map<String, String>> pointInfoList = new ArrayList<>();

	    // Extract point details from the asset's "points" array instead of the history
	    JsonNode pointsArray = asset.path("points");

	    if (pointsArray.isArray() && pointsArray.size() > 0) {
	        for (JsonNode pointElement : pointsArray) {
	            String pointName = pointElement.path("pointName").asText();
	            pointNames.add(pointName);

	            // Extract point details including unitSymbol
	            String dataType = pointElement.path("dataType").asText();
	            String unit = pointElement.path("unit").asText();
	            String unitSymbol = pointElement.path("unitSymbol").asText();

	            Map<String, String> pointInfo = new HashMap<>();
	            pointInfo.put("pointName", pointName);
	            pointInfo.put("dataType", dataType);
	            pointInfo.put("unit", unit);
	            pointInfo.put("unitSymbol", unitSymbol);
	            pointInfoList.add(pointInfo);
	        }
	    }

	    // Process assetHistory to populate monthlyDataMap (remains unchanged)
	    if (assetHistory.isArray() && assetHistory.size() > 0) {
	        for (JsonNode assetHistoryElement : assetHistory) {
	            JsonNode pointArray = assetHistoryElement.path("pointData");

	            if (pointArray.isArray() && pointArray.size() > 0) {
	                for (JsonNode pointArrayElement : pointArray) {
	                    String pointName = pointArrayElement.path("displayName").asText();
	                    JsonNode valueArray = pointArrayElement.path("values");
	                    if (valueArray.isArray() && valueArray.size() > 0) {
	                        for (JsonNode value : valueArray) {
	                            String dataTime = convertToTimezone(value.path("dataTime").asText(), targetZone, formatter);
	                            LocalDateTime localDateTime = LocalDateTime.parse(dataTime, formatter);

	                            String monthYear = localDateTime.getYear() + "-" + String.format("%02d", localDateTime.getMonthValue());
	                            String data = value.path("data").asText();

	                            monthlyDataMap
	                                .computeIfAbsent(monthYear, k -> new HashMap<>())
	                                .computeIfAbsent(dataTime, k -> new HashMap<>())
	                                .put(pointName, data);
	                        }
	                    }
	                }
	            }
	        }
	    }

	    // For each month, create a CSV file and write data (same as before)
	    for (Map.Entry<String, Map<String, Map<String, String>>> monthlyEntry : monthlyDataMap.entrySet()) {
	        String monthYear = monthlyEntry.getKey();
	        Map<String, Map<String, String>> dataMap = monthlyEntry.getValue();

	        String csvFilePath = assetFolderPath + File.separator + "AssetHistory_" + monthYear + ".csv";
	        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8)) {
	            writer.write("DateTime");
	            for (String pointName : pointNames) {
	                writer.write("," + pointName);
	            }
	            writer.write("\n");

	            for (Map.Entry<String, Map<String, String>> entry : dataMap.entrySet()) {
	                String dataTime = entry.getKey();
	                Map<String, String> values = entry.getValue();

	                writer.write(dataTime);
	                for (String pointName : pointNames) {
	                    writer.write("," + values.getOrDefault(pointName, ""));
	                }
	                writer.write("\n");
	            }

	            writer.flush();
	        } catch (IOException e) {
	            throw new IOException("Error writing CSV for " + monthYear, e);
	        }
	    }

	    // Create a second CSV file to store point info (including unitSymbol)
	    String pointInfoCsvFilePath = assetFolderPath + File.separator + "PointInfo.csv";
	    try (OutputStreamWriter pointInfoWriter = new OutputStreamWriter(new FileOutputStream(pointInfoCsvFilePath), StandardCharsets.UTF_8)) {
	        // Write CSV header for point info
	        pointInfoWriter.write("PointName,DataType,Unit,UnitSymbol\n");

	        // Write data rows for point info
	        for (Map<String, String> pointInfo : pointInfoList) {
	            pointInfoWriter.write(pointInfo.get("pointName") + ",");
	            pointInfoWriter.write(pointInfo.get("dataType") + ",");
	            pointInfoWriter.write(pointInfo.get("unit") + ",");
	            pointInfoWriter.write(pointInfo.get("unitSymbol") + "\n");
	        }

	        pointInfoWriter.flush();
	    } catch (IOException e) {
	        throw new IOException("Error writing PointInfo CSV", e);
	    }

	    // Create the third CSV file for asset details (same as before)
	    String assetDetailsCsvFilePath = assetFolderPath + File.separator + "AssetDetails.csv";
	    try (OutputStreamWriter assetDetailsWriter = new OutputStreamWriter(new FileOutputStream(assetDetailsCsvFilePath), StandardCharsets.UTF_8)) {
	        assetDetailsWriter.write("DisplayName,TypeName,CreatedOn,OwnerName,Model,Make,Status,UnderMaintenance\n");

	        assetDetailsWriter.write(asset.path("displayName").asText() + ",");
	        assetDetailsWriter.write(asset.path("typeName").asText() + ",");
	        assetDetailsWriter.write(convertToTimezone(asset.path("createdOn").asText(), targetZone, formatter) + ",");
	        assetDetailsWriter.write(asset.path("clientName").asText() + ",");
	        assetDetailsWriter.write(asset.path("model").asText() + ",");
	        assetDetailsWriter.write(asset.path("make").asText() + ",");
	        assetDetailsWriter.write(asset.path("status").asText() + ",");
	        assetDetailsWriter.write(asset.path("underMaintenance").asText() + "\n");

	        assetDetailsWriter.flush();
	    } catch (IOException e) {
	        throw new IOException("Error writing AssetDetails CSV", e);
	    }
	}

}
