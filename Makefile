.PHONY: install build test local-invoke local-api deploy clean

# Install dependencies
install:
	python3 -m venv venv
	. venv/bin/activate && pip install -r requirements.txt

# Build SAM application
build:
	sam build

# Test locally by invoking function
local-invoke:
	sam local invoke RorkHonestEatsApiFunction --event events/event.json

# Start local API Gateway
local-api:
	sam local start-api

# Deploy to AWS (dev environment)
deploy:
	sam deploy --config-env default

# Deploy to prod environment
deploy-prod:
	sam deploy --config-env prod

# Deploy with guided setup (first time)
deploy-guided:
	sam deploy --guided

# Clean build artifacts
clean:
	rm -rf .aws-sam
	rm -rf venv
	find . -type d -name __pycache__ -exec rm -r {} +
	find . -type f -name "*.pyc" -delete

