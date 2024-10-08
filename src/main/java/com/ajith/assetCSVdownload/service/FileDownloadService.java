package com.ajith.assetCSVdownload.service;


import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;

public interface FileDownloadService {
	public JsonNode fetchAssetList() throws Exception ;
	public JsonNode fetchAssetHistoryList(JsonNode asset)throws Exception;
	public void generateForEachAssetsCsv(JsonNode asset, JsonNode assetHistory,String baseFolderPath) throws IOException;
}
