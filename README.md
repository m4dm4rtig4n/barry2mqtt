# barry2mqtt

Usage :

```
ACCESS_TOKEN=""
MQTT_HOST='' 
MQTT_PORT="1883"                # Optionnal default => 1883
MQTT_PREFIX="barry"             # Optionnal default => barry
MQTT_CLIENT_ID="barry"          # Optionnal default => barry
MQTT_USERNAME='barryUsername'   # Optionnal
MQTT_PASSWORD='barryPassword'   # Optionnal
CYCLE=3600                      # Optionnak min => 3600, default => 3600

docker run -it -e ACCESS_TOKEN="${ACCESS_TOKEN}" \
    -e MQTT_HOST="${MQTT_HOST}" \
    -e MQTT_PORT="${MQTT_PORT}" \
    -e MQTT_PREFIX="${MQTT_PREFIX}" \
    -e MQTT_CLIENT_ID="${MQTT_CLIENT_ID}" \
    -e MQTT_USERNAME="${MQTT_USERNAME}" \
    -e MQTT_PASSWORD="${MQTT_PASSWORD}" \
-e CYCLE=10 \
m4dm4rtig4n/barry2mqtt:0.1
```
