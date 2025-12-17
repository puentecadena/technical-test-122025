# Makefile para ETL Entregas de Productos

.PHONY: build test test-all test-config test-reader test-transformer test-writer run clean help

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
	@echo "  make run              - Ejecutar el ETL"
	@echo "  make clean            - Limpiar contenedores huérfanos"
	@echo ""
	@echo "Ejemplo: make test-config"

# Construir imagen Docker
build:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) build etl

# Ejecutar todos los tests
test:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl pytest tests/ -v

# Tests individuales
test-config:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl pytest tests/test_config_loader.py -v

test-reader:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl pytest tests/test_reader.py -v

test-transformer:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl pytest tests/test_transformer.py -v

test-writer:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) run --rm etl pytest tests/test_writer.py -v

# Ejecutar ETL
run:
	@echo "Usando: $(CONTAINER_ENGINE)"
	$(COMPOSE) up pyspark-etl

# Limpiar
clean:
	$(COMPOSE) down --remove-orphans
