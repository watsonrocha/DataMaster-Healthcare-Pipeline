"""
Extração de dados de APIs públicas de saúde.

APIs utilizadas:
  • IBGE — Dados demográficos por estado brasileiro
  • Disease.sh — Dados epidemiológicos (COVID-19) por país
  • Dados Abertos SUS — Estabelecimentos de saúde (quando disponível)

Nenhuma das APIs requer autenticação.
"""

import logging
import json
import os
from datetime import datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# Timeout em segundos para requisições HTTP
REQUEST_TIMEOUT = 15


def fetch_ibge_estados() -> list[dict]:
    """Busca dados dos estados brasileiros via API do IBGE."""
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
    logger.info("Buscando estados brasileiros da API IBGE: %s", url)

    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    estados = resp.json()

    result = []
    for e in estados:
        result.append(
            {
                "estado_id": e["id"],
                "sigla": e["sigla"],
                "nome_estado": e["nome"],
                "regiao": e["regiao"]["nome"],
            }
        )

    logger.info("IBGE: %d estados obtidos", len(result))
    return result


def fetch_covid_brasil() -> list[dict]:
    """Busca dados de COVID-19 do Brasil via Disease.sh (API pública)."""
    url = "https://disease.sh/v3/covid-19/countries/Brazil"
    logger.info("Buscando dados COVID-19 do Brasil: %s", url)

    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    result = [
        {
            "pais": data.get("country", "Brazil"),
            "casos_total": data.get("cases", 0),
            "mortes_total": data.get("deaths", 0),
            "recuperados": data.get("recovered", 0),
            "ativos": data.get("active", 0),
            "casos_por_milhao": data.get("casesPerOneMillion", 0),
            "mortes_por_milhao": data.get("deathsPerOneMillion", 0),
            "testes_total": data.get("tests", 0),
            "populacao": data.get("population", 0),
            "data_atualizacao": datetime.fromtimestamp(data.get("updated", 0) / 1000).strftime("%Y-%m-%d %H:%M:%S"),
        }
    ]

    logger.info("COVID-19 Brasil: %d casos, %d mortes", result[0]["casos_total"], result[0]["mortes_total"])
    return result


def fetch_covid_historico(dias: int = 30) -> list[dict]:
    """Busca histórico de COVID-19 do Brasil (últimos N dias)."""
    url = f"https://disease.sh/v3/covid-19/historical/Brazil?lastdays={dias}"
    logger.info("Buscando histórico COVID-19 (%d dias): %s", dias, url)

    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    timeline = data.get("timeline", {})
    cases = timeline.get("cases", {})
    deaths = timeline.get("deaths", {})
    recovered = timeline.get("recovered", {})

    result = []
    for date_str, total_cases in cases.items():
        result.append(
            {
                "data": date_str,
                "casos_acumulados": total_cases,
                "mortes_acumuladas": deaths.get(date_str, 0),
                "recuperados_acumulados": recovered.get(date_str, 0),
            }
        )

    logger.info("Histórico COVID-19: %d registros diários", len(result))
    return result


def fetch_all_api_data() -> dict[str, list[dict]]:
    """Busca dados de todas as APIs e retorna em um dicionário."""
    api_data = {}

    try:
        api_data["estados_ibge"] = fetch_ibge_estados()
    except Exception as e:
        logger.warning("Falha ao buscar IBGE: %s", e)
        api_data["estados_ibge"] = []

    try:
        api_data["covid_brasil"] = fetch_covid_brasil()
    except Exception as e:
        logger.warning("Falha ao buscar COVID-19: %s", e)
        api_data["covid_brasil"] = []

    try:
        api_data["covid_historico"] = fetch_covid_historico(30)
    except Exception as e:
        logger.warning("Falha ao buscar histórico COVID-19: %s", e)
        api_data["covid_historico"] = []

    return api_data


def save_api_data(api_data: dict[str, list[dict]], output_dir: str) -> dict[str, str]:
    """Salva dados da API em arquivos JSON no Data Lake (Bronze)."""
    os.makedirs(output_dir, exist_ok=True)
    paths = {}

    for name, records in api_data.items():
        if not records:
            continue
        path = os.path.join(output_dir, f"api_{name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        paths[name] = path
        logger.info("API '%s': %d registros salvos em %s", name, len(records), path)

    return paths
