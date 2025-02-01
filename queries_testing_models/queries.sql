--- Partilhar visão da dim snapshot para uma rota:
SELECT *
FROM `data-eng-dev-437916.data_eng_project_group2_snapshots.snp_dim_routes`
WHERE sk_route = 'a1ee4cebffbe0450d0eb87ff331050a9'
ORDER BY dbt_valid_from asc;
--- Para uma paragem:
SELECT *
FROM `data-eng-dev-437916.data_eng_project_group2_snapshots.snp_dim_routes`
WHERE CONTAINS_SUBSTR(stops, 'R Manuel Casimiro Q1')


--- No dia 18/01, quantas viagens feitas passaram na R Manuel Casimiro Q1?
WITH STOP AS (
    SELECT *
    FROM 
      `data-eng-dev-437916.data_eng_project_group2_marts.dim_routes`, 
      UNNEST(stops) AS stop,
      UNNEST(municipalities) AS municipalities
    WHERE 
      stop.stop_name LIKE 'R Manuel Casimiro Q1'
      AND dbt_valid_to IS NULL
)

SELECT trip_date
, STOP.route_name
, COUNT(DISTINCT STOP.line_id) AS NR_LINHAS
, COUNT(DISTINCT STOP.municipality_name) AS NR_MUNICIPIOS
, COUNT(DISTINCT sk_trip) AS NR_VIAGENS
, SUM(total_trip_distance) AS total_trip_distance
, SUM(total_trip_time) AS total_trip_time
, SUM(total_trip_distance) / SUM(total_trip_time) AS avg_speed
FROM `data-eng-dev-437916.data_eng_project_group2_marts.fact_historical_trips` A
INNER JOIN STOP
ON A.sk_route = STOP.sk_route
WHERE A.trip_date >= '2025-01-18' AND A.trip_date <= CURRENT_DATE()
GROUP BY trip_date
, STOP.route_name
ORDER BY STOP.route_name, A.trip_date;


-- Rotas que param em estação X
SELECT 
  route_id,
  route_name,
  stop.stop_id,
  stop.stop_name,
  stop.stop_operational_status
FROM `data-eng-dev-437916.data_eng_project_group2_marts.dim_routes`,
UNNEST(stops) AS stop
WHERE stop.stop_id = '80084'


-- Rotas que servem munícipio X 
SELECT DISTINCT 
    m.municipality_name, 
    r.route_id, 
    r.route_name, 
FROM 
    `data-eng-dev-437916.data_eng_project_group2_marts.dim_routes` r,
    UNNEST(municipalities) AS m
WHERE 
    m.municipality_name = 'Mafra';


-- As duas questões anteriores mas com algum detalhe adicional na filtragem
WITH dim_routes AS (
  SELECT 
    sk_route,
    route_id,
    route_name,
    line_id,
    m.municipality_name,
    --stop.stop_id,
    --stop.stop_name
  FROM `data-eng-dev-437916.data_eng_project_group2_marts.dim_routes`,
  --UNNEST(stops) AS stop,
  UNNEST(municipalities) AS m
),

dim_dates AS (
  SELECT 
    date,
    day_type,
    period,
    period_name
  FROM `data-eng-dev-437916.data_eng_project_group2_marts.dim_calendar_dates` d
),

dim_trips AS (
  SELECT 
    sk_trip,
    direction_id,
    direction_name
  FROM `data-eng-dev-437916.data_eng_project_group2_marts.dim_trips` t
)


SELECT DISTINCT
  r.line_id
  ,r.route_id
  ,r.route_name
  --,t.direction_id
  --,r.stop_id
  --,r.stop_name
  --,r.municipality_name
  --,f.total_trip_distance
  --,f.average_trip_speed
FROM `data-eng-dev-437916.data_eng_project_group2_marts.fact_historical_trips` f
INNER JOIN dim_routes r ON f.sk_route = r.sk_route
INNER JOIN dim_dates d ON f.trip_date = d.date
INNER JOIN dim_trips t ON f.sk_trip = t.sk_trip
--WHERE r.line_id = '1005' -- Em que day_type circula a linha 1005?
--WHERE r.municipality_name = 'Loures' AND d.period = 3  -- Rotas que servem município X durante o período de Verão
--WHERE r.municipality_name = 'Lisboa' AND t.direction_id = 1 -- Rotas que servem o município x com direction_id = 1 (Outbound)
WHERE r.municipality_name = 'Sintra' AND f.trip_date = '2025-01-22' -- Rotas que circularam na passada quarta-feira, no município X


-- Cálculo da velocidade média realizado pela viagem XXX, por dia:
SELECT *
, ROUND(CASE
             WHEN total_trip_time > 0 THEN total_trip_distance / total_trip_time
             ELSE NULL
             END, 3) AS trip_avg_speed
FROM `data-eng-dev-437916.data_eng_project_group2_marts.fact_historical_trips`
WHERE sk_trip = '39cc2ea8575836663f810a6e9a0316ce'