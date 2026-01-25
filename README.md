# UFC Scraper

Este scraper extrae datos de peleas de UFC desde Tapology.com y los almacena en MongoDB.

## Características

- Scrapea eventos de UFC (nombre, fecha, ubicación, etc.)
- Extrae información detallada de peleas (peleadores, peso, categoría, etc.)
- Ingresa a páginas individuales de peleas para extraer datos comparativos:
  - Rankings UFC
  - Records en la pelea
  - Últimas 5 peleas
  - Betting odds
  - Nacionalidad
  - Fighting out of
  - Edad en la pelea
  - Peso, altura, reach
  - Gimnasios

## Instalación Local

1. Instalar dependencias:
```bash
cd scraper
pip install -r requirements.txt
```

2. Configurar variables de entorno:
Crear un archivo `.env` en el directorio `scraper/` con:
```
MONGODB_URI=tu_conexion_mongodb_aqui
```

## Uso

### Modo descubrimiento (scraper automático de eventos futuros)
```bash
cd scraper
python -m scrapy crawl ufc
```

### Scraper de un evento específico
```bash
python -m scrapy crawl ufc -a EVENT_ID=136026
```

### Scraper de resultados de un evento
```bash
python -m scrapy crawl ufc -a EVENT_ID=136026 -a MODE=results
```

### Sin scrapear detalles de peleas (más rápido)
```bash
python -m scrapy crawl ufc -a SKIP_BOUT_DETAILS=true
```

## GitHub Actions

El scraper se ejecuta automáticamente cada día a las 00:00 UTC mediante GitHub Actions.

### Configurar secretos en GitHub:

1. Ve a tu repositorio en GitHub
2. Settings → Secrets and variables → Actions
3. Click en "New repository secret"
4. Añadir:
   - Name: `MONGODB_URI`
   - Secret: Tu URI de conexión a MongoDB

### Ejecución manual:

1. Ve a la pestaña "Actions" en GitHub
2. Selecciona "UFC Scraper Workflow"
3. Click en "Run workflow"
4. Opcional: especifica un EVENT_ID o MODE

## Estructura de Datos en MongoDB

### Colección: `events`
```json
{
  "id": 136026,
  "name": "UFC 325",
  "event_date": "2026-02-08",
  "location": "Las Vegas, Nevada"
}
```

### Colección: `bouts`
```json
{
  "id": 1074233,
  "event_id": 136026,
  "fighters": {
    "red": {
      "fighter_name": "Alexander Volkanovski",
      "ufc_ranking": {"position": 1, "division": "Featherweight"},
      "record_at_fight": {"wins": 27, "losses": 4, "draws": 0},
      "height": {"cm": 168},
      "reach": {"cm": 182},
      "betting_odds": {"line": "-160", "description": "Slight Favorite"}
    },
    "blue": { /* ... */ }
  }
}
```

## Notas

- El scraper respeta robots.txt y usa delays entre requests
- Los eventos anteriores a 2026-01-01 son ignorados automáticamente
- Después de 10 eventos antiguos consecutivos, el scraper detiene la paginación
