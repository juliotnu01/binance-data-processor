# Define los comandos que no crean archivos
.PHONY: help init up down logs clean

# Comando por defecto: muestra la ayuda
help:
	@echo "Comandos disponibles:"
	@echo "make init - Inicializa el archivo .env con el CLUSTER_ID de Kafka."
	@echo "make up - Inicializa (si es necesario) y levanta todos los servicios."
	@echo "make down - Detiene todos los servicios."
	@echo "make logs - Muestra los logs de todos los servicios en tiempo real."
	@echo "make clean - Detiene los servicios y elimina volúmenes y el archivo .env."

# Inicializa el entorno
init:
	@echo ">>> Inicializando entorno..."
	@./init.sh

# Levanta los servicios (depende de init)
up: init
	@echo ">>> Levantando servicios con Docker Compose..."
	@docker-compose up -d
	@echo ">>> ¡Servicios en marcha! Usa 'make logs' para ver la salida."
	@SYMBOL_COUNT=$$(wc -l < ./data/symbols.txt); \
	echo ">>> Encontrados $$SYMBOL_COUNT símbolos. Escalando ingestores..."; \
	docker-compose up -d --scale data_ingestor_multi=$$SYMBOL_COUNT
	@echo ">>> El cliente web está disponible en http://localhost:8080"

# Detiene los servicios
down:
	@echo ">>> Deteniendo servicios..."
	@docker-compose down

# Muestra los logs
logs:
	@docker-compose logs -f

# Limpia todo el proyecto (útil para empezar de cero)
clean: down
	@echo ">>> Eliminando volúmenes de Docker y el archivo .env..."
	@docker-compose down -v --remove-orphans
	@rm -f .env
	@echo ">>> Limpieza completada."