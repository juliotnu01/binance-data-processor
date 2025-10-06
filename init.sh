#!/bin/bash

ENV_FILE=".env"

generate_cluster_id() {
    if [ ! -f "$ENV_FILE" ]; then
        echo "Creando archivo .env..."
        if [ -f ".env.example" ]; then
            cp .env.example .env
        else
            touch "$ENV_FILE"
        fi
    fi

    # Verifica si KAFKA_CLUSTER_ID ya está definido (no comentado)
    if ! grep -q "^KAFKA_CLUSTER_ID=" "$ENV_FILE"; then
        echo "Generando KAFKA_CLUSTER_ID y añadiéndolo a .env..."
        
        # Elimina cualquier línea de KAFKA_CLUSTER_ID (comentada o no)
        sed -i '' '/^#*KAFKA_CLUSTER_ID=/d' "$ENV_FILE"
        
        # Genera nuevo CLUSTER_ID usando Python
        if command -v python3 &> /dev/null; then
            KAFKA_CLUSTER_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
        elif command -v python &> /dev/null; then
            KAFKA_CLUSTER_ID=$(python -c "import uuid; print(uuid.uuid4())")
        else
            echo "Error: No se encontró Python para generar el UUID." >&2
            exit 1
        fi
        
        echo "KAFKA_CLUSTER_ID=$KAFKA_CLUSTER_ID" >> "$ENV_FILE"
        echo "Nuevo KAFKA_CLUSTER_ID generado."
    else
        echo "KAFKA_CLUSTER_ID ya existe en el archivo .env."
    fi
}

generate_cluster_id
echo "Inicialización completada."