cd ../../
mvn clean install
cd examples
docker build -t streaming .

cd ../infrastructure-local/docker

docker-compose -f docker-compose-full.yml up
