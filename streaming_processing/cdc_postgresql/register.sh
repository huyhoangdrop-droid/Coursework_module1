curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector-config.json

# curl -X DELETE http://localhost:8083/connectors/postgres-connector2

#if window
'''Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors" `
  -ContentType "application/json" `
  -InFile "connector-config.json"
'''