NUM_NODES=5
NUM_CONNECTIONS=3
python src/util.py $NUM_NODES topologies/dolev.yaml dolev $NUM_CONNECTIONS
docker compose build
docker compose up

