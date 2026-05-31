"""Testes unitarios para o extrator de APIs publicas."""

import os
import sys
import json
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "DTM", "DTM"))

from src.data_extraction.api_extractor import (
    fetch_ibge_estados,
    fetch_covid_brasil,
    fetch_covid_historico,
    fetch_all_api_data,
    save_api_data,
)


class TestIBGE:
    def test_retorna_27_estados(self):
        """API do IBGE deve retornar exatamente 27 estados."""
        estados = fetch_ibge_estados()
        assert len(estados) == 27

    def test_campos_estado(self):
        """Cada estado deve ter os campos obrigatorios."""
        estados = fetch_ibge_estados()
        for e in estados:
            assert "estado_id" in e
            assert "sigla" in e
            assert "nome_estado" in e
            assert "regiao" in e

    def test_sigla_duas_letras(self):
        """Sigla do estado deve ter 2 letras."""
        estados = fetch_ibge_estados()
        for e in estados:
            assert len(e["sigla"]) == 2
            assert e["sigla"].isalpha()

    def test_regioes_validas(self):
        """Regioes devem ser as 5 regioes brasileiras."""
        estados = fetch_ibge_estados()
        regioes = {e["regiao"] for e in estados}
        esperadas = {"Norte", "Nordeste", "Sudeste", "Sul", "Centro-Oeste"}
        assert regioes == esperadas


class TestCOVID:
    def test_dados_brasil(self):
        """Deve retornar dados do Brasil."""
        dados = fetch_covid_brasil()
        assert len(dados) == 1
        assert dados[0]["pais"] == "Brazil"
        assert dados[0]["casos_total"] > 0
        assert dados[0]["populacao"] > 0

    def test_historico_30_dias(self):
        """Historico deve ter aproximadamente 30 registros."""
        hist = fetch_covid_historico(30)
        assert len(hist) >= 25  # API pode retornar alguns dias a menos
        assert len(hist) <= 35

    def test_campos_historico(self):
        """Cada registro do historico deve ter os campos obrigatorios."""
        hist = fetch_covid_historico(5)
        for h in hist:
            assert "data" in h
            assert "casos_acumulados" in h
            assert "mortes_acumuladas" in h


class TestFetchAll:
    def test_retorna_todas_fontes(self):
        """fetch_all_api_data deve retornar dados das 3 fontes."""
        data = fetch_all_api_data()
        assert "estados_ibge" in data
        assert "covid_brasil" in data
        assert "covid_historico" in data


class TestSaveApiData:
    def test_salva_json(self):
        """Deve salvar dados como arquivos JSON."""
        data = {
            "teste": [{"campo1": "valor1", "campo2": 42}],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = save_api_data(data, tmpdir)
            assert "teste" in paths
            assert os.path.exists(paths["teste"])
            with open(paths["teste"], encoding="utf-8") as f:
                saved = json.load(f)
            assert len(saved) == 1
            assert saved[0]["campo1"] == "valor1"

    def test_ignora_lista_vazia(self):
        """Nao deve criar arquivo para lista vazia."""
        data = {"vazio": []}
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = save_api_data(data, tmpdir)
            assert "vazio" not in paths
