NUM_NODES=10
python src/util.py $NUM_NODES topologies/dolev.yaml dolev
docker compose build
docker compose up

