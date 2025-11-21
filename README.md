# Real-time Fraud Detection System

A production-ready, open-source real-time fraud detection system using Kafka, PyCaret, Airflow, and MLflow.

## Quickstart

### Prerequisites
- Docker & Docker Compose
- Python 3.10+ (for local development outside Docker)

### Running the System
1. **Start Infrastructure**:
   ```bash
   docker-compose up -d --build
   ```
   This starts Kafka, Zookeeper, Airflow, MLflow, Prometheus, Grafana, Scorer, and Producer.

2. **Verify Services**:
   - **Airflow**: [http://localhost:8080](http://localhost:8080) (User: `admin`, Pass: `admin`)
   - **MLflow**: [http://localhost:5000](http://localhost:5000)
   - **Grafana**: [http://localhost:3000](http://localhost:3000) (User: `admin`, Pass: `admin`)
   - **Prometheus**: [http://localhost:9090](http://localhost:9090)
   - **Scorer API**: [http://localhost:8000/docs](http://localhost:8000/docs)

3. **Trigger Training**:
   - Go to Airflow UI.
   - Enable and trigger the `train_and_register` DAG.
   - Verify in MLflow that a model is registered.

4. **Simulate Fraud**:
   - The `producer` service is automatically generating transactions.
   - Check the `alerts` topic or the Scorer logs:
     ```bash
     docker-compose logs -f scorer
     ```

## Architecture

The system consists of the following components:
- **Data Ingestion**: Kafka (`transactions` topic).
- **Stream Processing**: Python service using `kafka-python` and `FastAPI`.
- **Model Training**: PyCaret for AutoML, MLflow for tracking/registry.
- **Orchestration**: Airflow for retraining and batch jobs.
- **Monitoring**: Prometheus & Grafana.

See [docs/architecture.mermaid](docs/architecture.mermaid) for a diagram.

## Runbook

### How to Simulate Drift
1. Modify `services/producer/producer.py` to change the distribution of generated data (e.g., increase amounts).
2. Rebuild and restart the producer:
   ```bash
   docker-compose up -d --build producer
   ```
3. The `drift_detection_monitor` DAG (scheduled daily) will eventually detect this (simulated in current logic) and trigger retraining.

### Model Rollback
1. Go to MLflow UI.
2. Find the previous version of the model in "Models" -> "FraudDetectionModel".
3. Promote the previous version to "Production" stage.
4. Restart the Scorer service to load the new production model (if dynamic loading is not enabled).

## Security Notes
- **Kafka**: Currently configured with PLAINTEXT for local dev. In production, enable SSL/SASL.
- **API**: The Scorer API is unauthenticated. In production, put behind an API Gateway or add OAuth2/API Key middleware.
- **Secrets**: Credentials are in `docker-compose.yml`. Move to Docker Secrets or Environment Variables for production.

## API Specification
See [docs/api_spec.md](docs/api_spec.md) or visit `/docs` on the running Scorer service.
