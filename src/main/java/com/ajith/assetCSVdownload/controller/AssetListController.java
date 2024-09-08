package com.ajith.assetCSVdownload.controller;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ajith.assetCSVdownload.service.FileDownloadService;

@RestController
@RequestMapping("/api/v3/assets")
public class AssetListController {

    @Autowired
    private FileDownloadService assetListService;

    @GetMapping("/download")
    public ResponseEntity<?> downloadAssetListCsv() {
        try {
            var assetsNode = assetListService.fetchAssetList();
            byte[] csvData = assetListService.generateCsv(assetsNode);

            HttpHeaders csvHeaders = new HttpHeaders();
            csvHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            csvHeaders.setContentDispositionFormData("attachment", "assets.csv");

            return new ResponseEntity<>(new ByteArrayInputStream(csvData).readAllBytes(), csvHeaders, HttpStatus.OK);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
        }
    }
    @GetMapping("datatrends/download")
    public ResponseEntity<?> downloadAssetDataHistoryListCsv() {
        try {
            var assets = assetListService.fetchAssetList();
            List<File> csvFiles = new ArrayList<>();

            // Create folder for CSV files
            String folderPath = "C:/ajith/csv/AssetDataHistory";
            Files.createDirectories(Paths.get(folderPath));

            for (var asset : assets) {
                String assetId = asset.get("displayName").asText();
                var assetHistory = assetListService.fetchAssetHistoryList(asset);
                assetListService.generateForEachAssetsCsv(asset, assetHistory,folderPath);

            }

            return ResponseEntity.ok("CSV files created successfully in folder: " + folderPath);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
        }
    }

    
    
}
