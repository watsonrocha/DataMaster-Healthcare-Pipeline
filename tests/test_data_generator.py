"""Testes unitarios para o gerador de dados simulados."""

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "DTM", "DTM"))

from src.data_extraction.data_generator import (
    _generate_cpf,
    _random_blood_pressure,
    generate_healthcare_records,
    save_as_csv,
    save_as_json,
)


class TestGenerateCPF:
    def test_formato_cpf(self):
        """CPF deve ter formato XXX.XXX.XXX-XX."""
        cpf = _generate_cpf()
        assert len(cpf) == 14
        assert cpf[3] == "."
        assert cpf[7] == "."
        assert cpf[11] == "-"

    def test_cpf_apenas_digitos(self):
        """CPF deve conter apenas digitos (alem de pontos e hifen)."""
        cpf = _generate_cpf()
        apenas_numeros = cpf.replace(".", "").replace("-", "")
        assert apenas_numeros.isdigit()
        assert len(apenas_numeros) == 11

    def test_cpfs_diferentes(self):
        """Dois CPFs gerados devem ser diferentes (probabilisticamente)."""
        cpfs = {_generate_cpf() for _ in range(100)}
        assert len(cpfs) > 90


class TestBloodPressure:
    def test_formato_pressao(self):
        """Pressao arterial deve ter formato XXX/XXX."""
        bp = _random_blood_pressure()
        parts = bp.split("/")
        assert len(parts) == 2
        assert parts[0].isdigit()
        assert parts[1].isdigit()

    def test_faixas_pressao(self):
        """Valores de pressao devem estar em faixas realistas."""
        for _ in range(100):
            bp = _random_blood_pressure()
            sistolica, diastolica = map(int, bp.split("/"))
            assert 80 <= sistolica <= 180
            assert 50 <= diastolica <= 120


class TestGenerateRecords:
    def test_quantidade_registros(self):
        """Deve gerar a quantidade de registros solicitada."""
        records = generate_healthcare_records(10)
        assert len(records) == 10

    def test_campos_obrigatorios(self):
        """Cada registro deve ter todos os campos obrigatorios."""
        records = generate_healthcare_records(5)
        campos = {
            "patient_id",
            "cpf",
            "nome",
            "email",
            "telefone",
            "idade",
            "sexo",
            "cidade",
            "estado",
            "blood_pressure",
            "heart_rate",
            "temperatura",
            "saturacao_o2",
            "diagnostico",
            "medicamento",
            "timestamp",
        }
        for rec in records:
            assert campos.issubset(rec.keys())

    def test_faixas_valores(self):
        """Valores numericos devem estar em faixas validas."""
        records = generate_healthcare_records(50)
        for rec in records:
            assert 18 <= rec["idade"] <= 95
            assert rec["sexo"] in ("M", "F")
            assert 50 <= rec["heart_rate"] <= 130
            assert 35.5 <= rec["temperatura"] <= 40.5
            assert 88 <= rec["saturacao_o2"] <= 100

    def test_patient_id_formato(self):
        """patient_id deve ter formato PAC-XXXXXX."""
        records = generate_healthcare_records(5)
        for rec in records:
            assert rec["patient_id"].startswith("PAC-")
            assert rec["patient_id"][4:].isdigit()


class TestSaveFiles:
    def test_save_csv(self):
        """Deve salvar registros como CSV valido."""
        records = generate_healthcare_records(3)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.csv")
            result = save_as_csv(records, path)
            assert os.path.exists(result)
            with open(result, encoding="utf-8") as f:
                lines = f.readlines()
            assert len(lines) == 4  # header + 3 registros

    def test_save_json(self):
        """Deve salvar registros como JSON Lines."""
        records = generate_healthcare_records(3)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.jsonl")
            result = save_as_json(records, path)
            assert os.path.exists(result)
            with open(result, encoding="utf-8") as f:
                lines = f.readlines()
            assert len(lines) == 3
