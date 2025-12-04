# Unit Testing

## Test Results

- **Server**: 36 tests passing
- **Worker**: 19 tests passing
- **Total**: 55 tests

## Test Structure

```
server/tests/unit/
├── test_data_models.py      # Pydantic model validation
├── test_database.py          # Database operations
└── test_api_endpoints.py     # API endpoints

kafka_worker/tests/unit/
├── test_recommendation_engine.py  # Recommendation logic
└── test_kafka_worker_db.py        # Database operations
```

## Running Tests

### Automatic (during build)
```bash
docker compose up --build -d
```

### Manual execution
```bash
# Server tests
docker exec asset-recommendation-server pytest /app/tests/unit/ -v

# Worker tests
docker exec asset-recommendation-worker pytest /app/tests/unit/ -v

# With coverage
docker exec asset-recommendation-server pytest /app/tests/unit/ --cov=server
docker exec asset-recommendation-worker pytest /app/tests/unit/ --cov=.
```

## Notes

- Tests run automatically during Docker build
- Build continues even if tests fail (for debugging)
- All external dependencies (DB, Kafka) are mocked
- Check build logs for test results
