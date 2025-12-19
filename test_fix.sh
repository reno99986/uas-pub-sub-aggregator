#!/bin/bash
# Quick rebuild and test script

echo "=== Stopping containers ===" 
sudo docker compose down

echo "=== Rebuilding aggregator ===" 
sudo docker compose build aggregator

echo "=== Starting all services ===" 
sudo docker compose up -d

echo "=== Waiting 25 seconds for services to start ===" 
sleep 25

echo "=== Checking for consumer errors ===" 
sudo docker compose logs aggregator | grep -E "(consumer_error|consumer_processed)" | tail -20

echo ""
echo "=== Checking statistics ===" 
sudo docker compose exec aggregator curl -s http://localhost:8080/stats | python3 -m json.tool

echo ""
echo "=== Service status ===" 
sudo docker compose ps
