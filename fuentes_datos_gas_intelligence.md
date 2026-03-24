# Gas Commercializer Intelligence Engine — Catálogo de Fuentes de Datos

**Versión:** 1.0 — Marzo 2026  
**Objetivo:** Inventario exhaustivo de fuentes concretas, URLs, formatos y estrategia de scraping para el Data Lake (SP0).

---

## Resumen por Tier

| Tier | Definición | Cantidad de fuentes | Estrategia |
|------|-----------|---------------------|-----------|
| 1 — Automatizable | API o CSV descargable con URL estable | 7 | Script Python + cron diario/semanal |
| 2 — Semi-automático | Portal web sin API, pero con exports descargables | 5 | Descarga manual periódica + script de normalización |
| 3 — Manual | PDFs, informes no estructurados, datos hipotéticos | 4 | Template Excel + validación de schema al importar |

---

## TIER 1 — Automatizable

### F01. Producción de Petróleo y Gas (SESCO Upstream)

| Campo | Detalle |
|-------|---------|
| **Organismo** | Secretaría de Energía — Dirección Nac. de Exploración y Producción |
| **Dataset** | Producción de Petróleo y Gas (SESCO) |
| **URL catálogo** | `https://datos.gob.ar/dataset/energia-produccion-petroleo-gas-sesco` |
| **URL recurso CSV (desde 2019)** | `https://datos.gob.ar/dataset/energia-produccion-petroleo-gas-sesco/archivo/energia_3752bb79-7229-4a3b-8f61-c617bfb17677` |
| **URL recurso CSV (hasta 2008)** | `https://datos.gob.ar/dataset/energia-produccion-petroleo-gas-sesco/archivo/energia_f4cf0c95-68c7-476e-b279-89e0d43b1b71` |
| **Formato** | CSV |
| **Granularidad** | Mensual / yacimiento / concesión / provincia / empresa |
| **Campos clave** | Empresa, yacimiento, concesión, cuenca, provincia, producción de gas (miles m³), producción de petróleo (m³), formación, tipo de reservorio |
| **Frecuencia actualización** | Mensual (se actualiza el dataset completo) |
| **Histórico disponible** | Desde ~1990 hasta mes actual (rezago ~2 meses) |
| **Tabla destino en DL** | `produccion_diaria` (agregar por mes), `gas_asociado_ratio` (calcular GOR) |
| **Valor para el proyecto** | **CRÍTICO.** Es la fuente principal para el Supply Engine. Permite calcular GOR por yacimiento/cuenca, identificar gas asociado vs. libre, trackear ramp-up de producción petrolera en VM. |
| **Estrategia de scraping** | Descargar CSV completo vía URL directa del recurso en datos.gob.ar (el portal CKAN expone URLs estables). Script: `requests.get(url_csv)` → parsear con pandas → filtrar cuencas relevantes (Neuquina, Austral, Noroeste) → calcular campo `tipo_gas` (libre si yacimiento no tiene producción petrolera asociada, asociado si sí) → calcular GOR = gas_total / petroleo_total por yacimiento/mes → guardar en Parquet particionado. Cron: semanal (los datos se actualizan mensualmente pero el cron detecta el cambio). |

---

### F02. Producción por pozo y fracturas (SESCO + Cap IV)

| Campo | Detalle |
|-------|---------|
| **Organismo** | Secretaría de Energía |
| **Dataset** | Producción SESCO + Tight y Shale Capítulo IV |
| **URL recurso** | `https://datos.gob.ar/dataset/energia-produccion-petroleo-gas-sesco/archivo/energia_83a2b597-b087-4815-b17d-cd70990d6a79` |
| **Formato** | CSV |
| **Granularidad** | Mensual / pozo / formación / tipo reservorio |
| **Campos clave** | Pozo, formación (Vaca Muerta, etc.), tipo reservorio (tight/shale), longitud rama horizontal, etapas de fractura, tipo de terminación, toneladas de arena |
| **Tabla destino en DL** | Tabla auxiliar `pozos_no_convencional` para enriquecer Supply Engine |
| **Valor para el proyecto** | Permite anticipar ramp-up de producción no convencional. Los pozos con más etapas de fractura y mayor rama horizontal producen más gas asociado. Leading indicator. |
| **Estrategia de scraping** | Mismo mecanismo que F01. Descargar CSV, filtrar por cuenca Neuquina y formaciones de interés (Vaca Muerta, Loma Campana, etc.). Cron semanal. |

---

### F03. Regalías y Precios de Gas Natural (boca de pozo)

| Campo | Detalle |
|-------|---------|
| **Organismo** | Secretaría de Energía — Dir. Nac. de Transporte y Medición de Hidrocarburos |
| **Dataset** | Regalías de Petróleo Crudo, Gas Natural, GLP, Gasolina y Condensado |
| **URL catálogo** | `https://datos.gob.ar/dataset/energia-regalias-petroleo-crudo-gas-natural-glp-gasolina-condensado` |
| **URL recurso Gas Natural** | `https://datos.gob.ar/dataset/energia-regalias-petroleo-crudo-gas-natural-glp-gasolina-condensado/archivo/energia_451ef089-ef35-4a25-a87f-cc030b55083b` |
| **Formato** | CSV |
| **Granularidad** | Mensual / cuenca / empresa |
| **Campos clave** | Empresa, cuenca, provincia, volumen de gas (miles m³), precio en boca de pozo ($/MMBTU o $/miles m³), monto de regalías |
| **Frecuencia actualización** | Mensual (~2 meses de rezago) |
| **Tabla destino en DL** | `precios_boca_pozo` |
| **Valor para el proyecto** | **CRÍTICO para Supply Engine.** Es la fuente más confiable de precios de gas en boca de pozo por cuenca. Permite reconstruir la curva de costo de adquisición histórica. |
| **Estrategia de scraping** | Descargar CSV del recurso → parsear → pivotar para tener precio promedio ponderado por cuenca/mes → convertir unidades a USD/MMBTU si está en AR$ (usar serie de tipo de cambio del BCRA). Cron semanal. |

---

### F04. Precios de Gas Natural (series SE)

| Campo | Detalle |
|-------|---------|
| **Organismo** | Secretaría de Energía |
| **Dataset** | Precios de Gas Natural |
| **URL catálogo** | `https://datos.gob.ar/dataset/energia-precios-gas-natural` |
| **URL datos.energia** | `http://datos.energia.gob.ar/dataset/precios-de-gas-natural` |
| **Formato** | CSV |
| **Granularidad** | Mensual / cuenca / segmento |
| **Campos clave** | Fecha, cuenca, segmento de demanda, precio promedio ponderado |
| **Tabla destino en DL** | `precios_boca_pozo` (complementa F03) |
| **Valor para el proyecto** | Precio por segmento de demanda; complementa la vista de regalías con granularidad de segmento. |
| **Estrategia de scraping** | Mismo approach que F03. Descargar CSV directo, normalizar. Cron semanal. |

---

### F05. Balances de Gas (SESCO)

| Campo | Detalle |
|-------|---------|
| **Organismo** | Secretaría de Energía |
| **Dataset** | Balances de Gas — desde 2009 — SESCO Web |
| **URL recurso** | `https://datos.gob.ar/dataset/energia-produccion-petroleo-gas-sesco/archivo/energia_34415dd6-2dd6-4480-a464-62dcdcf6241b` |
| **Formato** | CSV |
| **Granularidad** | Mensual / punto de ingreso-egreso |
| **Campos clave** | Inyección, distribución, transporte, consumo por segmento, exportación, importación |
| **Tabla destino en DL** | `inyeccion_sistema`, `consumo_diario` (agregar para mensual) |
| **Valor para el proyecto** | Visión completa del balance oferta/demanda del sistema. Permite detectar excedentes/déficits estructurales. |
| **Estrategia de scraping** | CSV directo. Normalizar nombres de puntos de inyección para match con tabla de capacidad de transporte. Cron semanal. |

---

### F06. Datos Operativos de Gas Natural (ENARGAS)

| Campo | Detalle |
|-------|---------|
| **Organismo** | ENARGAS vía datos.energia.gob.ar |
| **Dataset** | Datos Operativos de Gas Natural |
| **URL datos.energia** | `http://datos.energia.gob.ar/dataset/datos-operativos-de-gas-natural` |
| **Formato** | CSV |
| **Granularidad** | Mensual / distribuidora / segmento |
| **Campos clave** | Gas entregado por distribuidora, gas a grandes usuarios, total sistema, GLP indiluido |
| **Frecuencia actualización** | Semestral/anual (última actualización hace ~11 meses según portal) |
| **Tabla destino en DL** | `consumo_diario` (como mensual por distribuidora) |
| **Valor para el proyecto** | Base del Demand Forecast Engine. Consumo por distribuidora y segmento. |
| **Estrategia de scraping** | CSV directo. OJO: actualización infrecuente — complementar con datos de despacho (F07). Cron semanal para detectar updates. |

---

### F07. Clima — NOAA GHCNd / CDO API

| Campo | Detalle |
|-------|---------|
| **Organismo** | NOAA — National Centers for Environmental Information (NCEI) |
| **Dataset** | Global Historical Climatology Network daily (GHCNd) |
| **URL API** | `https://www.ncei.noaa.gov/cdo-web/api/v2/` |
| **URL docs** | `https://www.ncdc.noaa.gov/cdo-web/webservices/v2` |
| **URL registro token** | `https://www.ncdc.noaa.gov/cdo-web/token` |
| **Formato** | JSON (API REST) |
| **Granularidad** | Diaria / estación meteorológica |
| **Campos clave** | TMIN, TMAX, PRCP (temp mínima, máxima, precipitación) |
| **Estaciones relevantes** | Buenos Aires (Ezeiza/Aeroparque), Rosario, Córdoba, Mendoza, Neuquén, Bahía Blanca, Mar del Plata, Tucumán, Comodoro Rivadavia |
| **Frecuencia actualización** | Diaria |
| **Tabla destino en DL** | `clima` (+ calcular HDD/CDD offline) |
| **Valor para el proyecto** | **CRÍTICO para Demand Forecast.** Temperatura es el predictor #1 de demanda residencial de gas. HDD (Heating Degree Days) es la variable más importante. |
| **Estrategia de scraping** | Registrar token en CDO web → Script Python con requests: query por estación, dataset GHCND, datatypes TMIN/TMAX, rango de fechas. Máx 1 año por query. Rate limit: 5 req/seg, 10K req/día. Calcular HDD = max(18 - Tavg, 0) y CDD = max(Tavg - 24, 0) en post-procesamiento. Cron diario (solo trae el día anterior). Backfill histórico: loop por año desde 2010. |

**Ejemplo de query:**
```
GET https://www.ncei.noaa.gov/cdo-web/api/v2/data
  ?datasetid=GHCND
  &stationid=AR000087582  (Ezeiza)
  &startdate=2025-01-01
  &enddate=2025-01-31
  &datatypeid=TMIN,TMAX
  &units=metric
  &limit=1000
Headers: token: {tu_token}
```

---

## TIER 2 — Semi-automático

### F08. MEGSA — Reportes PPP (Precios, Producción y Programación)

| Campo | Detalle |
|-------|---------|
| **Organismo** | MEGSA — Mercado Electrónico de Gas S.A. |
| **Portal negociación** | `https://negociacion.megsa.ar/` |
| **Portal réplica despachos** | `https://replica.megsa.ar/` |
| **Reportes PPP (metodología)** | `https://negociacion.megsa.ar/usuario/visualizacion/VisualizacionMetodologia.aspx` |
| **Formato** | Portal web con login. Reportes descargables (presumiblemente XLSX/PDF). |
| **Granularidad** | Diaria / cuenca / segmento / tipo de contrato |
| **Campos clave** | Precio promedio ponderado por volumen asignado, por cuenca, segmento y tipo de contrato. Volúmenes solicitados, confirmados y asignados. |
| **Acceso** | **Requiere licencia de agente MEGSA.** Pluspetrol como productor/comercializador debería tener acceso. Verificar con equipo comercial. |
| **Tabla destino en DL** | `precios_megsa` |
| **Valor para el proyecto** | **MÁXIMO VALOR para Supply y Pricing.** Es la fuente más granular de precios reales de transacción del mercado de gas. Los reportes PPP dan precios por cuenca, segmento y tipo de contrato. La Réplica de Despachos tiene configuración física + contratos + despacho diario de todo el sistema. |
| **Estrategia de scraping** | (1) Verificar acceso Pluspetrol al portal. (2) Si hay export a XLSX/CSV: descarga manual semanal + script de normalización que valida schema y carga al DL. (3) Si solo hay visualización web: Selenium/Playwright scraper autenticado que navega los reportes y extrae tablas. (4) Alternativa: solicitar a MEGSA datos históricos en bulk (es servicio que ofrecen). **Drop-folder**: `data/raw/megsa/` con naming `megsa_ppp_YYYYMM.xlsx`. |

---

### F09. ENARGAS — Datos Operativos de Despacho

| Campo | Detalle |
|-------|---------|
| **Organismo** | ENARGAS |
| **URL portal** | `https://www.enargas.gob.ar/secciones/transporte-y-distribucion/datos-operativos-despacho.php` |
| **URL datos estadísticos** | `https://www.enargas.gob.ar/secciones/transporte-y-distribucion/datos-operativos.php` |
| **Formato** | Tablas HTML interactivas + PDFs con gráficos Power BI |
| **Granularidad** | Diaria (despacho) y mensual (estadísticos) por distribuidora, transportista, segmento |
| **Campos clave** | Gas entregado por distribuidora y segmento, gas recibido por transportista, capacidad de transporte firme, consumo de combustible, line pack |
| **Tabla destino en DL** | `consumo_diario`, `capacidad_transporte` |
| **Valor para el proyecto** | Complementa F06 con granularidad más fina. Los datos de despacho diario son valiosos para el Demand Forecast a nivel diario. |
| **Estrategia de scraping** | (1) Las tablas estadísticas son HTML con links a XLS. Scraper con BeautifulSoup para listar links → descargar XLS → parsear con openpyxl/pandas. (2) Los datos de despacho diario son PDFs con gráficos Power BI → más difícil, considerar solo si no se consigue granularidad diaria de otra fuente. **Drop-folder** para descarga manual: `data/raw/enargas/despacho/`. Cron que monitorea el folder y normaliza automáticamente. |

---

### F10. Partes Diarios de Gas Natural (ENARGAS vía datos.gob.ar)

| Campo | Detalle |
|-------|---------|
| **Organismo** | ENARGAS vía datos.energia.gob.ar |
| **Dataset** | Partes diarios de gas natural |
| **URL catálogo** | `https://datos.gob.ar/dataset/energia-partes-diarios-gas-natural` |
| **Formato** | CSV (pero última actualización hace >2 años según portal) |
| **Granularidad** | Diaria / operativo |
| **Campos clave** | Requerimientos y partes diarios operativos, importación y exportación, uso de capacidad por distribuidora |
| **Tabla destino en DL** | `consumo_diario`, `capacidad_transporte` |
| **Valor para el proyecto** | Datos operativos diarios del sistema de gas — ideal para Demand Forecast si está actualizado. |
| **Estrategia de scraping** | Intentar descarga CSV directa. Si está desactualizado (>2 años), marcar como fuente degradada y priorizar F09 como alternativa. Verificar estado al iniciar SP0. |

---

### F11. Despacho Diario (ENARGAS vía datos.energia.gob.ar)

| Campo | Detalle |
|-------|---------|
| **Organismo** | ENARGAS |
| **Dataset** | Despacho Diario (ENARGAS) |
| **URL** | `http://datos.energia.gob.ar/dataset/despachoenargas` |
| **Formato** | CSV |
| **Granularidad** | Diaria |
| **Campos clave** | Entregas e inyecciones, estimación demanda prioritaria, confirmaciones de gas, desbalances por segmento, eventos críticos, proyección semanal |
| **Tabla destino en DL** | `consumo_diario`, `inyeccion_sistema` |
| **Valor para el proyecto** | **MUY VALIOSO** si está actualizado. Desbalances por segmento es input directo para Risk Engine. |
| **Estrategia de scraping** | CSV directo si disponible. Verificar frescura del dato. Si OK: script de descarga + validación semanal. |

---

### F12. Capacidad de Transporte Contratada en Firme (ENARGAS)

| Campo | Detalle |
|-------|---------|
| **Organismo** | ENARGAS |
| **Dataset** | Transporte — Capacidad contratada en firme por cargador |
| **URL** | `http://datos.energia.gob.ar/dataset/transporte` |
| **Formato** | CSV |
| **Granularidad** | Por cargador / tramo |
| **Campos clave** | Cargador, tramo, gasoducto, capacidad firme contratada |
| **Tabla destino en DL** | `capacidad_transporte` |
| **Valor para el proyecto** | Esencial para Pricing Engine (costo de transporte) y Risk Engine (riesgo de capacidad). |
| **Estrategia de scraping** | CSV directo. Complementar con datos manuales de TGS/TGN para capacidad total vs. contratada. |

---

## TIER 3 — Manual

### F13. TGS / TGN — Informes de Capacidad y Concursos

| Campo | Detalle |
|-------|---------|
| **Organismo** | TGS (Transportadora de Gas del Sur) / TGN (Transportadora de Gas del Norte) |
| **URL TGS** | `https://www.tgs.com.ar/` (sección inversores / operaciones) |
| **URL TGN** | `https://www.tgn.com.ar/` |
| **Formato** | PDFs en reportes trimestrales, informes anuales. Presentaciones para inversores. |
| **Granularidad** | Trimestral / gasoducto / tramo |
| **Campos clave** | Capacidad nominal por gasoducto, utilización, tarifas de transporte, concursos abiertos, obras de expansión |
| **Tabla destino en DL** | `capacidad_transporte`, `concursos_abiertos` |
| **Valor para el proyecto** | Datos de capacidad son escasos y estas son las fuentes primarias. Tarifas de transporte son input directo del Pricing Engine. |
| **Estrategia de ingesta** | Template Excel (`template_capacidad_transporte.xlsx`) con columnas predefinidas. Carga manual trimestral post-lectura de informes. Script de validación verifica: campos obligatorios completos, tramo_id matchea catálogo, utilización entre 0-100%. |

---

### F14. Plan Gas.Ar / Programas de incentivo

| Campo | Detalle |
|-------|---------|
| **Organismo** | Secretaría de Energía |
| **URL** | `https://www.argentina.gob.ar/economia/energia` (sección hidrocarburos) |
| **Formato** | Resoluciones en Boletín Oficial (PDF), tablas en web. |
| **Granularidad** | Trimestral / productor |
| **Campos clave** | Productor, volumen comprometido (MMm³/d), precio incentivado (USD/MMBTU), vigencia, estado del programa |
| **Tabla destino en DL** | `plan_gas` |
| **Valor para el proyecto** | El Plan Gas.Ar establece un piso de precio para una porción significativa de la producción. Afecta la curva de oferta directamente — el gas bajo Plan Gas tiene precio fijo, no responde al mercado. |
| **Estrategia de ingesta** | Carga manual anual/semestral. Template Excel con campos de resolución, productor, volúmenes y precios. Actualizar cuando sale nueva resolución SE. |

---

### F15. INDEC — Actividad Económica Sectorial

| Campo | Detalle |
|-------|---------|
| **Organismo** | INDEC |
| **URL** | `https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-9-48` (Estimador Mensual de Actividad Económica — EMAE) |
| **Formato** | XLSX descargable |
| **Granularidad** | Mensual / sector de actividad |
| **Campos clave** | Índice de actividad industrial, construcción, comercio — proxies de demanda de gas industrial |
| **Tabla destino en DL** | `clientes_proxy` (como feature de actividad económica) |
| **Valor para el proyecto** | Proxy de demanda industrial. Si la actividad industrial crece, la demanda de gas industrial crece. Feature para Demand Forecast Engine. |
| **Estrategia de ingesta** | Descargar XLSX mensual de la web de INDEC (URL estable para serie histórica). Parsear con openpyxl, extraer índices relevantes (industria, construcción). Semi-automatizable: script que verifica si hay nueva versión del XLSX. |

---

### F16. Contratos Hipotéticos de Compra y Venta

| Campo | Detalle |
|-------|---------|
| **Organismo** | Interno — generado por equipo Pluspetrol SF |
| **Formato** | Excel |
| **Granularidad** | Por contrato |
| **Campos clave** | contrato_id, contraparte, tipo (compra/venta), cuenca, volumen firme, precio fórmula, take-or-pay, vigencia, punto de entrega/inyección |
| **Tabla destino en DL** | `contratos_compra`, `contratos_venta` |
| **Valor para el proyecto** | Sin esto, el sistema no puede simular operación. En fase inicial son hipotéticos (escenarios). A medida que avance el proyecto de comercializadora, se reemplazan con datos reales. |
| **Estrategia de ingesta** | Template Excel predefinido con validación de datos (listas desplegables para cuenca, tipo, etc.). Carga manual ad-hoc. |

---

## FUENTE AUXILIAR — Tipo de cambio

### F17. BCRA — Tipo de Cambio

| Campo | Detalle |
|-------|---------|
| **Organismo** | Banco Central de la República Argentina |
| **URL API** | `https://api.bcra.gob.ar/` |
| **Formato** | JSON (API REST abierta) |
| **Granularidad** | Diaria |
| **Campos clave** | Cotización USD/ARS (vendedor BNA) |
| **Tabla destino en DL** | `tipo_cambio` (tabla auxiliar para convertir precios AR$ a USD) |
| **Estrategia de scraping** | API REST abierta, no requiere autenticación. `GET https://api.bcra.gob.ar/estadisticas/v2.0/DatosVariable/4/{desde}/{hasta}` (variable 4 = TC minorista). Cron diario. |

---

## FUENTE BONUS — Calendario

### F18. Calendario Argentina (feriados + semana gas)

| Campo | Detalle |
|-------|---------|
| **Organismo** | Generado internamente |
| **Formato** | Script Python genera CSV |
| **Campos clave** | fecha, dia_semana, es_feriado, es_laborable, semana_gas (ENARGAS define semana gas de miércoles a martes), mes, trimestre, estacion |
| **Tabla destino en DL** | `calendario` |
| **Estrategia** | Script Python con paquete `holidays` (Argentina) + lógica manual para semana gas. Generar de 2010 a 2028 de una vez. Regenerar anualmente cuando se publican feriados del año nuevo. |

---

## Prioridades de implementación (Fase 0)

| Prioridad | Fuente | Razón |
|-----------|--------|-------|
| **P0 — Semana 1** | F01 (Producción SESCO), F07 (Clima NOAA), F18 (Calendario) | Las tres más fáciles de automatizar y más valiosas: producción para Supply, clima para Demand, calendario para ambos. |
| **P1 — Semana 2** | F03 (Regalías/Precios boca pozo), F05 (Balances gas), F17 (Tipo cambio BCRA) | Completan el Supply Engine con precios. Tipo de cambio es necesario para normalizar. |
| **P2 — Semana 3** | F06 (Datos operativos ENARGAS), F11 (Despacho diario), F12 (Capacidad transporte) | Demand side + transporte. Verificar frescura de los datos en portales. |
| **P3 — Semana 4** | F08 (MEGSA), F09 (ENARGAS despacho web), F15 (INDEC) | Semi-automáticas. MEGSA requiere verificar acceso Pluspetrol. |
| **P4 — Semana 5** | F13 (TGS/TGN), F14 (Plan Gas), F16 (Contratos hipotéticos) | Manuales. Preparar templates Excel y cargar datos iniciales. |

---

## Notas técnicas

### Estructura del drop-folder
```
sp0-datalake/
├── scrapers/
│   ├── f01_produccion_sesco.py
│   ├── f02_pozos_no_conv.py
│   ├── f03_regalias_precios.py
│   ├── f04_precios_gas.py
│   ├── f05_balances_gas.py
│   ├── f06_datos_operativos.py
│   ├── f07_clima_noaa.py
│   ├── f11_despacho_diario.py
│   ├── f12_capacidad_transporte.py
│   ├── f17_tipo_cambio_bcra.py
│   └── f18_calendario.py
├── loaders/
│   ├── normalize_sesco.py
│   ├── normalize_megsa.py
│   ├── normalize_enargas.py
│   ├── normalize_clima.py
│   └── validate_schema.py
├── templates/
│   ├── template_megsa_ppp.xlsx
│   ├── template_capacidad_transporte.xlsx
│   ├── template_plan_gas.xlsx
│   └── template_contratos.xlsx
├── schemas/
│   ├── produccion_diaria.json
│   ├── precios_megsa.json
│   ├── clima.json
│   └── ... (un schema JSON por tabla)
├── data/
│   ├── raw/           ← CSVs descargados, sin tocar
│   │   ├── sesco/
│   │   ├── megsa/     ← drop-folder para MEGSA manual
│   │   ├── enargas/
│   │   ├── noaa/
│   │   └── manual/    ← drop-folder para Tier 3
│   ├── processed/     ← normalizados, validados
│   └── snapshots/     ← Parquet inmutables con hash+timestamp
└── duckdb/
    └── gas_intel.duckdb
```

### Riesgos conocidos
1. **datos.gob.ar / datos.energia.gob.ar inestable:** El portal CKAN del gobierno tiene historial de caídas y cambios de URL sin aviso. Mitigation: guardar copia local de cada descarga, versionar URLs en config.
2. **MEGSA acceso:** Es el dato más valioso y el más difícil de obtener. Sin acceso de agente, solo se tienen los reportes PPP públicos (limitados). Escalar con equipo comercial Pluspetrol como P0.
3. **ENARGAS datos desactualizados:** Varios datasets en datos.gob.ar llevan >2 años sin actualización. Verificar frescura antes de invertir tiempo de scraper.
4. **Cambio de gobierno / reestructuración SE:** Las URLs y estructuras de datos del gobierno argentino cambian con cada cambio de gestión. Diseñar scrapers resilientes (detectar error → alertar → no fallar silenciosamente).
