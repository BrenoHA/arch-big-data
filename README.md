## 1. Build and run the containers of the structure with the apache, kafka, spark and cassandra:

In a terminal opened in the root directory of the project run the following commands:
```sh
cd kappa
docker-compose up -d
```

## 2. Build and run container that will send information to kafka (producer):
To start sending the information to kafka to be processed we need to create the producer container to that will generate the events.

```bash
# Considering that you are currently in the folder kappa/ from the previous command 
cd ../producer
docker-compose up -d
```

## 3. Perform Docker buildx prune to clean up unused build cache.

```bash
docker buildx prune -f
```

## 4. Starting sending information to kafka

Open a bash session in the producer container and start the script ```get-weather-data.py```:

```bash
docker exec -ti producer bash
    > cd app/; python generate-random-events.py
```

## 5. Start spark streaming
To start this step we must first check the IP of the cassandra container to see if we need to change it in our producer script, for this look at the IP in that appears running the following command:

```bash
# Run in a terminal of the host machine
docker network inspect mynetwork
```

If needed change the IP's in the line 2 of ```kappa/apps/create_structure_cassandra.py```:
```python
from cassandra.cluster import Cluster
clstr=Cluster(['172.19.0.2']) # Change this line if needed
session=clstr.connect()
```


```bash
docker exec -ti spark-master bash
    > cd ../spark-apps
    > python3 create_structure_cassandra.py

```