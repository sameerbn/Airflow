private List<JsonNode> fetchEtlStatus(LocalDate asOfDate, String space) {
    // ... existing code to generate token and URL ...

    ResponseEntity<String> resp = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(headers), String.class);
    String rawBody = resp.getBody();

    try {
        // Check if the output is actually JSON (starts with { or [)
        if (rawBody != null && (rawBody.trim().startsWith("{") || rawBody.trim().startsWith("["))) {
            JsonNode root = objectMapper.readTree(rawBody);
            return StreamSupport.stream(root.spliterator(), false).collect(Collectors.toList());
        } else {
            // Handle the "Correct" Non-JSON output by wrapping it
            log.info("ETL Tracker returned non-JSON output. Wrapping into synthetic JSON Node.");
            
            ObjectNode syntheticNode = objectMapper.createObjectNode();
            // Map the raw 'output' to a field your downstream flow can read
            syntheticNode.put("status", "SUCCESS"); 
            syntheticNode.put("rawOutput", rawBody);
            
            return Collections.singletonList(syntheticNode);
        }
    } catch (Exception e) {
        log.error("Failed to process ETL output: {}", rawBody);
        throw new IllegalStateException("Cannot parse ETL tracker response for " + space, e);
    }
}
