# fraud-detection-analytics-workflows
Airflow-orchestrated workflows for fraud analytics, daily reporting, and notifications.

launch container airflow container in Docker
	docker compose up airflow-init
	docker compose up -d
	
port-forward for Postgresql pod (minikube / kubernetes local cluster) access
	kubectl port-forward --address 0.0.0.0 svc/fraud-postgres-postgresql 15432:5432
	
Execute a test (exemple test_monitoring_checks)
	docker compose exec airflow-scheduler pytest /opt/airflow/dags/tests/test_monitoring_checks.py -v


