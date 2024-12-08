# Movies Reviews

## Seeting up the environment
Start docker and run the following:  
`docker compose up --build`

When you are done remember to stop and clear the resources:
` docker compose down`

## Ingestion

You can see the logs:  
`docker compose logs -f ingestion`

To ingest example data use:  
`python post_reviews.py`

## Processing

You can see the logs:  
`docker compose logs -f processing`


### API
You can see the logs: 
`docker compose logs -f api`

You can query the api service:  
- `curl -k -X GET http://localhost:8081/movies -H "Content-Type: application/json" | jq .`
- `curl -k -X GET http://localhost:8081/reviews -H "Content-Type: application/json" | jq .`

## Connecting to the resources

### Redis

You can connect to redis cli locally with:  
`redis-cli`

### Postgres

You can connect to postgres locally with:  

`psql --username app_user --password app_password --host localhost --port 5432 --dbname app_db`

### MinIO

You can interact with the bucket locally browsing:  
`http://localhost:9001`

