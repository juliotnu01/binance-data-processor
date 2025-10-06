#!/bin/bash

SYMBOLS_FILE="/app/data/symbols.txt"

while [ ! -f "$SYMBOLS_FILE" ]; do
  echo "🔍 Esperando a que el orquestador cree el archivo de símbolos..."
  sleep 2
done

echo "📄 Archivo de símbolos encontrado."
INDEX=$(echo $HOSTNAME | grep -oE '[0-9]+$')
SYMBOL=$(sed -n "${INDEX}p" "$SYMBOLS_FILE")

if [ -z "$SYMBOL" ]; then
  echo "🚨 No se encontró un símbolo para el índice $INDEX. Saliendo."
  exit 1
fi

echo "🚀 Este contenedor procesará el símbolo: $SYMBOL"
export SYMBOL=$SYMBOL
exec python app.py