The commands that i have used are presnt here :
docker-compose down 
docker-compose up -d
docker exec -it mongo2 mongosh --eval "rs.status()"
// for durabilty.py, usermodel.py and write_concern.py you can run it normally (Note that i ran on a virtual environment)
// following command to run python script in the same network as the cluster 
docker run --rm -it \\n  --network mongo-replica_mongonet \\n  -v "$(pwd)/scritps":/app/scripts \\n  -v /var/run/docker.sock:/var/run/docker.sock \\n  -w /app \\n  docker:24.0.5-dind /bin/sh -c "\\n    apk add --no-cache python3 py3-pip && \\n    pip3 install --no-cache-dir pymongo && \\n    python3 /app/scripts/failover.py"
docker run --rm -it \\n  --network mongo-replica_mongonet \\n  -v "$(pwd)/scritps":/app/scripts \\n  -w /app \\n  -e MONGO_URI="mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0&retryWrites=false" \\n  python:3.11-slim bash -lc "pip install pymongo && python /app/scripts/consistency.py"\n