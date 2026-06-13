# Ad Click Aggregator — developer entrypoints.
# See specs/001-ad-click-aggregator/quickstart.md for the full flow.

.PHONY: help test test-ruby test-spark test-flink fmt build build-lambdas build-flink build-glue tf-validate

ARTIFACTS_BUCKET ?= ad-click-artifacts-dev
LAMBDA_DIST := dist

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

test: test-ruby test-spark ## Run all locally-runnable unit tests (Ruby + Spark transform)

test-ruby: ## Run RSpec for the shared gem and both Lambdas
	cd services/shared          && bundle exec rspec
	cd services/click_processor && bundle exec rspec --tag ~integration
	cd services/query_service   && bundle exec rspec --tag ~integration

test-spark: ## Run PySpark reconciliation transform tests
	cd batch/reconciliation && python -m pytest -q

test-flink: ## Run Flink MiniCluster tests (requires Maven)
	cd stream/flink-aggregator && mvn -q test

fmt: ## Format Ruby (standard) and Terraform
	cd services/shared          && bundle exec standardrb --fix || true
	cd services/click_processor && bundle exec standardrb --fix || true
	cd services/query_service   && bundle exec standardrb --fix || true
	terraform -chdir=infra/terraform/envs/dev fmt -recursive ../..

tf-validate: ## terraform fmt -check + validate
	terraform -chdir=infra/terraform/envs/dev fmt -check -recursive ../..
	terraform -chdir=infra/terraform/envs/dev validate

build: build-lambdas build-flink build-glue ## Build all deployable artifacts

build-lambdas: ## Zip the Ruby Lambdas (shared gem vendored in)
	mkdir -p $(LAMBDA_DIST)
	scripts/build_lambda.sh click_processor
	scripts/build_lambda.sh query_service

build-flink: ## Package the Flink fat jar and upload to the artifacts bucket
	cd stream/flink-aggregator && mvn -q clean package
	aws s3 cp stream/flink-aggregator/target/flink-aggregator-1.0.0.jar \
		s3://$(ARTIFACTS_BUCKET)/flink/flink-aggregator-1.0.0.jar

build-glue: ## Upload the Glue PySpark script to the artifacts bucket
	aws s3 cp batch/reconciliation/job.py s3://$(ARTIFACTS_BUCKET)/glue/job.py
