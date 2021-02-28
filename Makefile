build:
	@echo "Creating zip file..."
	rm -rf ./target && mkdir ./target
	cp run.py ./target
	cp config.json ./target
	zip -r ./target/etl_app.zip etl_application


clean:
	@echo "Cleaning up..."
	rm -rf ./target