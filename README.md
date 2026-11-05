# fraud-detection-analytics-workflows
Airflow-orchestrated workflows for fraud analytics, daily reporting, and notifications.

launch container airflow container in Docker
	docker compose up airflow-init
	docker compose up -d
	
port-forward for Postgresql pod (minikube / kubernetes local cluster) access
	kubectl port-forward --address 0.0.0.0 svc/fraud-postgres-postgresql 15432:5432
	


