help:
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

clean: 			## Clean Docker containers and logs directory
	docker stop $$(docker ps -a -q) || true
	docker rm $$(docker ps -a -q) || true
	rm -r logs && mkdir logs && touch .gitkeep

start-airflow: 		## Start Airflow
	docker-compose up -d
	until $$(curl --output /dev/null --silent --head --fail http://localhost:8080) ; do \
        printf '.' ; \
        sleep 5 ; \
    done
	@echo ""
	@echo "\033[92m Airflow Up to http://localhost:8080 \033[0m"

stop-airflow: 		## Stop Airflow
	docker-compose down
