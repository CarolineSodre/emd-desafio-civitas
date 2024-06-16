-- Desafio CIVITAS - EMD

-- Análise Exploratória dos dados oferecidos contendo leituras de radar do município do Rio de Janeiro

-- ENTENDENDO OS DADOS:

-- Visualizando a tabela original

SELECT * 
FROM 
  `rj-cetrio.desafio.readings_2024_06`;

-- Realizando a contagem de Nulls de cada coluna

SELECT 
    COUNTIF(datahora IS NULL) AS datahora_null,
    COUNTIF(datahora_captura IS NULL) AS datahora_captura_null,
    COUNTIF(placa IS NULL) AS placa_null,
    COUNTIF(empresa IS NULL) AS empresa_null,
    COUNTIF(tipoveiculo IS NULL) AS tipoveiculo_null,
    COUNTIF(velocidade IS NULL) AS velocidade_null,
    COUNTIF(camera_numero IS NULL) AS camera_numero_null,
    COUNTIF(camera_latitude IS NULL) AS camera_latitude_null,
    COUNTIF(camera_longitude IS NULL) AS camera_longitude_null
FROM 
  `rj-cetrio.desafio.readings_2024_06`;

-- Análise dos tipos de Veículo

SELECT
  tipoveiculo,
  COUNT(*) AS total_tipoveiculo
FROM 
  `rj-cetrio.desafio.readings_2024_06`
GROUP BY tipoveiculo;

-- Análise dos tipos de Empresa

SELECT
  empresa,
  COUNT(*) AS total_empresa
FROM 
  `rj-cetrio.desafio.readings_2024_06`
GROUP BY empresa;

-- Análise dos valores de camera_numero

SELECT
  camera_numero,
  COUNT(*) AS count_camera_numero
FROM 
  `rj-cetrio.desafio.readings_2024_06`
GROUP BY
  camera_numero;

-- Analise dos valores mínimos e máximos de Latitude

SELECT
  MIN(camera_latitude) AS camera_latitude_min,
  MAX(camera_latitude) AS camera_latitude_max,
FROM
  `rj-cetrio.desafio.readings_2024_06`;

SELECT
  MIN(camera_latitude) AS camera_latitude_min,
  MAX(camera_latitude) AS camera_latitude_max,
FROM
  `rj-cetrio.desafio.readings_2024_06`
WHERE
  camera_latitude != 0;

-- Analise dos valores mínimos e máximos de Longitude

SELECT
  MIN(camera_longitude) AS camera_longitude_min,
  MAX(camera_longitude) AS camera_longitude_max,
FROM 
  `rj-cetrio.desafio.readings_2024_06`;

SELECT
  MIN(camera_longitude) AS camera_longitude_min,
  MAX(camera_longitude) AS camera_longitude_max,
FROM 
  `rj-cetrio.desafio.readings_2024_06`
WHERE
  camera_longitude < 0;

-- Análise do range de Velocidade

SELECT
  MIN(velocidade) AS velocidade_min,
  MAX(velocidade) AS velocidade_max,
FROM 
  `rj-cetrio.desafio.readings_2024_06`;

-- Contanto quantos veículos foram registrados com velocidade acima de 150

SELECT COUNT(*) AS velocidade_maior_150
FROM 
  `rj-cetrio.desafio.readings_2024_06`
WHERE 
 velocidade > 150;

-- Análise os tipos de placas e quantas vezes repetem

SELECT
  placa,
  COUNT(*) AS total_placa
FROM 
  `rj-cetrio.desafio.readings_2024_06`
GROUP BY
  placa;

-- TRANSFORMANDO OS DADOS TEMPORAIS:

-- Verificando se todas as datas terminam com UTC

SELECT
  COUNT(*) AS 
    total_registros,
  COUNT(*) - COUNTIF(RIGHT(CAST(datahora AS STRING), 3) != 'UTC' OR RIGHT(CAST(datahora_captura AS STRING), 3) != 'UTC') AS total_registros_nao_utc
FROM
  `rj-cetrio.desafio.readings_2024_06`;

-- Criando uma View com os dados transformados

CREATE OR REPLACE VIEW temporary_table.vw_dados_new AS
SELECT
  *,
  EXTRACT(YEAR FROM TIMESTAMP(datahora)) AS ano,
  EXTRACT(MONTH FROM TIMESTAMP(datahora)) AS mes,
  EXTRACT(DAY FROM TIMESTAMP(datahora)) AS dia,
  EXTRACT(HOUR FROM TIMESTAMP(datahora)) AS horas,
  EXTRACT(MINUTE FROM TIMESTAMP(datahora)) AS minutos,
  EXTRACT(SECOND FROM TIMESTAMP(datahora)) AS segundos
FROM
  `rj-cetrio.desafio.readings_2024_06`;

SELECT * 
FROM 
  `projeto-placas-clonadas.temporary_table.vw_dados_new`;

-- ANALISANDO OS DADOS TEMPORAIS:

-- Análise dos valores disponíveis de Ano, Mês e dia

SELECT
  MIN(ano) AS ano_min,
  MAX(ano) AS ano_max,
FROM 
  `projeto-placas-clonadas.temporary_table.vw_dados_new`;

SELECT
  MIN(mes) AS mes_min,
  MAX(mes) AS mes_max,
FROM 
  `projeto-placas-clonadas.temporary_table.vw_dados_new`;

SELECT
  MIN(dia) AS dia_min,
  MAX(dia) AS dia_max,
FROM 
  `projeto-placas-clonadas.temporary_table.vw_dados_new`;

-- Análise dos valores disponíveis de Hora, Minutos e Segundos

SELECT
  MIN(horas) AS horas_min,
  MAX(horas) AS horas_max,
FROM 
  `projeto-placas-clonadas.temporary_table.vw_dados_new`;

SELECT
  MIN(minutos) AS minutos_min,
  MAX(minutos) AS minutos_max,
FROM 
  `projeto-placas-clonadas.temporary_table.vw_dados_new`;

SELECT
  MIN(segundos) AS segundos_min,
  MAX(segundos) AS segundos_max,
FROM 
  `projeto-placas-clonadas.temporary_table.vw_dados_new`;

--  RETIRANDO OS VALORES NULOS E CALCULANDO QUANTIDADES IMPORTANTES:

-- Criando uma view apenas com as colunas relevantes para o problema, retirando os valores nulos de 
-- latitude, longitude e velocidade

CREATE OR REPLACE VIEW temporary_table.auxiliary_dataset AS
SELECT
  placa,
  datahora, 
  camera_latitude,
  camera_longitude,
FROM
  `rj-cetrio.desafio.readings_2024_06`
WHERE
  -- Removendo os regristros em que camera_latitude e velocidade são nulos, e camera_longitude é positivo ou nulo
  camera_latitude != 0
  AND camera_longitude < 0
  AND velocidade != 0;

-- Checando a view

SELECT
  *,
FROM 
  `projeto-placas-clonadas.temporary_table.auxiliary_dataset`;

-- Calculando a distância e o tempo levado entre pontos subsequentes, e então calculando a velocidade

CREATE OR REPLACE VIEW temporary_table.final_view AS
SELECT
  s.placa AS placa,
  s.datahora AS starting_time,
  e.datahora AS end_time,
  -- Criando pontos geográficos usando as coordenadas de latitude e longitude
  ST_GeogPoint(s.camera_longitude, s.camera_latitude) AS starting_point,
  ST_GeogPoint(e.camera_longitude, e.camera_latitude) AS end_point,
  -- Calculando a diferença de tempo levado entre o starting_point e end_point em segundos, usando a função TIMESTAMP_DIFF
  TIMESTAMP_DIFF(e.datahora, s.datahora, SECOND) AS time_taken,
  -- Calculando a distancia em metros entre o starting_point e end_point, usando a função ST_Distance
  ST_Distance(ST_GeogPoint(s.camera_longitude, s.camera_latitude), ST_GeogPoint(e.camera_longitude, e.camera_latitude), true)
  AS travelled_distance,
  -- Calculando a velocidade média (em m/s) a partir da distância percorrida e do tempo levado
  ST_Distance(ST_GeogPoint(s.camera_longitude, s.camera_latitude), ST_GeogPoint(e.camera_longitude, e.camera_latitude), true)
  / TIMESTAMP_DIFF(e.datahora, s.datahora, SECOND) AS average_speed
FROM (
  SELECT
    placa,
    datahora,
    camera_longitude,
    camera_latitude,
    -- Retornando o próximo valor de datahora para a mesma placa, ordenado pela datahora
    LEAD(datahora) OVER(PARTITION BY placa ORDER BY datahora) AS end_time,
    -- Retornando a próxima longitude para a mesma placa, ordenada pela datahora
    LEAD(camera_longitude) OVER(PARTITION BY placa ORDER BY datahora) AS end_longitude,
    -- Retornando a próxima latitude para a mesma placa, ordenada pela datahora
    LEAD(camera_latitude) OVER(PARTITION BY placa ORDER BY datahora) AS end_latitude
  FROM `projeto-placas-clonadas.temporary_table.auxiliary_dataset`
) s
-- Criando uma relação entre registros consecutivos de uma mesma placa de veículo, com base na ordem temporal (datahora)
JOIN (
  SELECT
    placa,
    datahora,
    camera_longitude,
    camera_latitude
  FROM `projeto-placas-clonadas.temporary_table.auxiliary_dataset`
) e
ON
  s.placa = e.placa
  AND s.end_time = e.datahora
  AND s.end_longitude = e.camera_longitude
  AND s.end_latitude = e.camera_latitude
WHERE
  s.datahora < e.datahora;

-- Checando a View

SELECT
  *,
FROM 
  `projeto-placas-clonadas.temporary_table.final_view`;

-- Filtrando apenas os casos com velocidade acimda de 150 km/hr

SELECT
    placa,
    COUNT(*) AS total_placa
FROM 
    `projeto-placas-clonadas.temporary_table.final_view`
WHERE 
  -- Passando 150 km/hr para m/s
    average_speed > 150/3.6
GROUP BY
    placa;
















