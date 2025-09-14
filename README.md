# Misinfo Verifier

A microservices-based misinformation detection and verification system that crawls web content, extracts claims, and provides verification capabilities.

## Architecture

This system uses a microservices architecture with the following components:

- **API Gateway**: FastAPI-based REST API for submitting URLs and querying results
- **Crawler Service**: Fetches and extracts text content from web pages
- **Cleaner Normalizer**: Normalizes and cleans extracted text, detects language
- **Claim Extractor**: Extracts verifiable claims from normalized text
- **Retriever** (M3+): Retrieves relevant evidence for claims
- **Verifier** (M4+): Verifies claims against evidence
- **Scoring Aggregator** (M5+): Aggregates verification scores

## Technology Stack

- **Backend**: Python 3.11, FastAPI, Motor (MongoDB async driver)
- **Message Queue**: Apache Kafka
- **Database**: MongoDB 7
- **Cache**: Redis 7
- **Containerization**: Docker, Docker Compose

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Git

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd misinfo-verifier
```

2. Start the services:
```bash
docker-compose up -d
```

3. Create MongoDB indexes:
```bash
docker exec -it misinfo-verifier-mongo-1 mongosh --quiet --eval "use misinfo; db.pages.createIndex({url:1},{unique:true}); db.claims.createIndex({claim_id:1},{unique:true}); db.pages.createIndex({domain:1});"
```

4. Verify the system is running:
```bash
curl "http://localhost:8000/health"
```

## API Endpoints

### Submit URL for Processing
```bash
curl -X POST "http://localhost:8000/submit-url" \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com"}'
```

### List Pages
```bash
curl "http://localhost:8000/pages?limit=20"
```

### List Claims
```bash
curl "http://localhost:8000/claims?limit=20"
```

### Filter Claims by Domain
```bash
curl "http://localhost:8000/claims?limit=20&domain=example.com"
```

### Health Check
```bash
curl "http://localhost:8000/health"
```

## Data Flow

1. **URL Submission**: URLs are submitted via the API Gateway
2. **Crawling**: Crawler service fetches and extracts text content
3. **Normalization**: Cleaner service normalizes text and detects language
4. **Claim Extraction**: Claim extractor identifies verifiable statements
5. **Storage**: All data is stored in MongoDB with Redis for deduplication

## Configuration

Environment variables can be set in the `.env` file:

```env
# Kafka Configuration
KAFKA_BOOTSTRAP=kafka:9092
TOPIC_PAGES_RAW=pages.raw
TOPIC_PAGES_CLEANED=pages.cleaned
TOPIC_CLAIMS_EXTRACTED=claims.extracted

# MongoDB Configuration
MONGO_URI=mongodb://mongo:27017
MONGO_DB=misinfo
COLL_PAGES=pages
COLL_CLAIMS=claims

# Redis Configuration
REDIS_URL=redis://redis:6379/0
REDIS_URL_SEEN=redis://redis:6379/1
REDIS_SEEN_URLS_KEY=seen:urls
REDIS_SEEN_CLAIMS_KEY=seen:claims
```

## Development

### Project Structure

```
misinfo-verifier/
├── docker-compose.yml          # Service orchestration
├── .env                        # Environment configuration
├── common/                     # Shared dependencies
│   ├── requirements.txt        # Python dependencies
│   └── base_settings.py       # Common configuration
├── api-gateway/               # REST API service
│   ├── app/main.py            # FastAPI application
│   └── Dockerfile
├── crawler-service/           # Web crawling service
│   ├── app/worker.py          # Kafka consumer
│   ├── app/extract.py         # Content extraction
│   └── Dockerfile
├── cleaner-normalizer/        # Text normalization service
│   ├── app/worker.py          # Text processing worker
│   └── Dockerfile
├── claim-extractor/           # Claim extraction service
│   ├── app/worker.py          # NLP-based claim extraction
│   └── Dockerfile
├── retriever/                 # Evidence retrieval (M3+)
│   ├── app/worker.py
│   └── Dockerfile
├── verifier/                  # Claim verification (M4+)
│   ├── app/worker.py
│   └── Dockerfile
├── scoring-aggregator/        # Score aggregation (M5+)
│   ├── app/worker.py
│   └── Dockerfile
└── tools/
    └── seed_urls.txt          # Initial URLs for testing
```

### Building Services

To rebuild a specific service:
```bash
docker-compose up --build -d <service-name>
```

### Viewing Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f api-gateway
```

### Stopping Services

```bash
docker-compose down
```

## Monitoring

### Service Status
```bash
docker-compose ps
```

### Database Queries

Connect to MongoDB:
```bash
docker exec -it misinfo-verifier-mongo-1 mongosh misinfo
```

Connect to Redis:
```bash
docker exec -it misinfo-verifier-redis-1 redis-cli
```

## Roadmap

- **M3**: Implement evidence retrieval service
- **M4**: Add claim verification capabilities
- **M5**: Implement scoring and aggregation
- **Future**: Web UI, advanced NLP models, real-time notifications

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with `docker-compose up --build`
5. Submit a pull request

## License

[Add your license information here]

## Support

For issues and questions, please [create an issue](link-to-issues) in the repository.
