"""
Gerador de dados simulados de saúde para demonstração do pipeline.

Gera registros realistas de pacientes incluindo dados sensíveis (CPF, nome,
email) para demonstrar técnicas de mascaramento e conformidade com a LGPD.
"""

import random
import csv
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

try:
    from faker import Faker

    fake = Faker("pt_BR")
except ImportError:
    fake = None


def _generate_cpf() -> str:
    """Gera CPF formatado (fictício)."""
    nums = [random.randint(0, 9) for _ in range(9)]
    for _ in range(2):
        val = sum((len(nums) + 1 - i) * v for i, v in enumerate(nums)) % 11
        nums.append(0 if val < 2 else 11 - val)
    cpf = "".join(map(str, nums))
    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"


def _random_blood_pressure() -> str:
    sistolica = random.randint(80, 180)
    diastolica = random.randint(50, 120)
    return f"{sistolica}/{diastolica}"


def generate_healthcare_records(n: int = 500) -> list[dict]:
    """Gera *n* registros simulados de pacientes."""
    records = []
    base_date = datetime.now() - timedelta(days=30)

    for i in range(n):
        ts = base_date + timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        if fake:
            nome = fake.name()
            email = fake.email()
            telefone = fake.phone_number()
            cidade = fake.city()
            estado = fake.state_abbr()
        else:
            nome = f"Paciente_{i:04d}"
            email = f"paciente{i}@email.com"
            telefone = f"(11) 9{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
            cidade = random.choice(
                ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Salvador", "Recife", "Fortaleza"]
            )
            estado = random.choice(["SP", "RJ", "MG", "PR", "BA", "PE", "CE"])

        record = {
            "patient_id": f"PAC-{random.randint(100000, 999999)}",
            "cpf": _generate_cpf(),
            "nome": nome,
            "email": email,
            "telefone": telefone,
            "idade": random.randint(18, 95),
            "sexo": random.choice(["M", "F"]),
            "cidade": cidade,
            "estado": estado,
            "blood_pressure": _random_blood_pressure(),
            "heart_rate": random.randint(50, 130),
            "temperatura": round(random.uniform(35.5, 40.5), 1),
            "saturacao_o2": random.randint(88, 100),
            "diagnostico": random.choice(
                [
                    "Hipertensão",
                    "Diabetes Tipo 2",
                    "Asma",
                    "Infecção Respiratória",
                    "Arritmia Cardíaca",
                    "Check-up Rotina",
                    "Fratura",
                    "Dengue",
                    "COVID-19",
                    "Gripe",
                ]
            ),
            "medicamento": random.choice(
                [
                    "Losartana",
                    "Metformina",
                    "Salbutamol",
                    "Amoxicilina",
                    "Atenolol",
                    "Paracetamol",
                    "Ibuprofeno",
                    "Dipirona",
                    None,
                    None,
                ]
            ),
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        }
        records.append(record)

    return records


def save_as_csv(records: list[dict], path: str) -> str:
    """Salva registros como CSV."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    return path


def save_as_json(records: list[dict], path: str) -> str:
    """Salva registros como JSON Lines."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return path


def generate_and_save(n: int = 500, output_dir: str = None) -> dict[str, str]:
    """Gera dados e salva em múltiplos formatos (CSV e JSON)."""
    if output_dir is None:
        from config.settings import Config

        output_dir = Config.STORAGE["bronze"]

    records = generate_healthcare_records(n)
    half = len(records) // 2

    csv_path = save_as_csv(records[:half], os.path.join(output_dir, "pacientes_batch1.csv"))
    json_path = save_as_json(records[half:], os.path.join(output_dir, "pacientes_batch2.jsonl"))

    return {"csv": csv_path, "json": json_path, "total_records": len(records)}
