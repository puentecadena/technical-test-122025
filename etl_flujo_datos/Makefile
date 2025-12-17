# Makefile para ETL Entregas de Productos

.PHONY: build test test-all test-config test-reader test-transformer test-writer run run-custom dev clean help

# Detectar motor de contenedores disponible
# Prioridad: docker > nerdctl.lima
DOCKER := $(shell command -v docker 2> /dev/null)
NERDCTL := $(shell command -v nerdctl.lima 2> /dev/null)

ifdef DOCKER
    COMPOSE := docker compose
    CONTAINER_ENGINE := docker
else ifdef NERDCTL
    COMPOSE := nerdctl.lima compose
    CONTAINER_ENGINE := nerdctl.lima
else
    $(error No se encontró docker ni nerdctl.lima. Por favor instale uno de ellos.)
endif

# Variables
IMAGE = etl-pyspark:latest

# Help
help:
	@echo "Motor de contenedores detectado: $(CONTAINER_ENGINE)"
	@echo ""
	@echo "Comandos disponibles:"
	@echo "  make build            - Construir imagen Docker"
	@echo "  make test             - Ejecutar todos los tests"
	@echo "  make test-config      - Ejecutar tests de config_loader"
	@echo "  make test-reader      - Ejecutar tests de reader"
	@echo "  make test-transformer - Ejecutar tests de transformer"
	@echo "  make test-writer      - Ejecutar tests de writer"
	@echo "  make run              - Ejecutar el ETL con configuración por defecto"
	@echo "  make run-custom       - Ejecutar el ETL con parámetros personalizados"
	@echo "  make dev              - Iniciar contenedor de desarrollo interactivo"
	@echo "  make clean            - Limpiar contenedores huérfanos"
	@echo ""
	@echo "Ejemplo: make test-config"

# Construir imagen Docker
build:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) build etl-dev

# Ejecutar todos los tests
test:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl-test

# Tests individuales
test-config:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl-dev pytest tests/test_config_loader.py -v

test-reader:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl-dev pytest tests/test_reader.py -v

test-transformer:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl-dev pytest tests/test_transformer.py -v

test-writer:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl-dev pytest tests/test_writer.py -v

# Ejecutar ETL con configuración por defecto
run:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl-run

# Ejecutar ETL con parámetros personalizados
# Uso: make run-custom START=2025-01-01 END=2025-06-30 COUNTRY=GT
run-custom:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl-dev python src/main.py \
		--config config/config.yaml \
		$(if $(START),--start-date $(START),) \
		$(if $(END),--end-date $(END),) \
		$(if $(COUNTRY),--country $(COUNTRY),)

# Desarrollo interactivo
dev:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl-dev bash

# Limpiar
clean:
	$(COMPOSE) down --remove-orphans
