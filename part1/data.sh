docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic binance-raw \
  --from-beginning

http://localhost:8080
