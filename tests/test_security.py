"""Testes unitarios para modulos de seguranca (RBAC e mascaramento)."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "DTM", "DTM"))

from src.security.access_control import AccessController


class TestAccessController:
    def setup_method(self):
        self.ac = AccessController()

    def test_roles_existem(self):
        """Deve ter 4 perfis de acesso definidos."""
        assert len(self.ac.ROLES) == 4
        assert "admin" in self.ac.ROLES
        assert "analista" in self.ac.ROLES
        assert "cientista_dados" in self.ac.ROLES
        assert "visitante" in self.ac.ROLES

    def test_admin_acesso_total(self):
        """Admin deve ter todas as permissoes."""
        perms = self.ac.get_permissions("admin")
        assert "read" in perms
        assert "write" in perms
        assert "delete" in perms
        assert "mask" in perms
        assert "export" in perms

    def test_visitante_apenas_leitura(self):
        """Visitante deve ter apenas permissao de leitura."""
        perms = self.ac.get_permissions("visitante_01")
        assert perms == ["read"]

    def test_analista_sem_delete(self):
        """Analista nao deve ter permissao de delete."""
        assert self.ac.has_permission("analista_01", "read")
        assert self.ac.has_permission("analista_01", "write")
        assert not self.ac.has_permission("analista_01", "delete")

    def test_cientista_com_export(self):
        """Cientista de dados deve ter permissao de exportacao."""
        assert self.ac.has_permission("cientista_01", "export")
        assert self.ac.has_permission("cientista_01", "read")
        assert not self.ac.has_permission("cientista_01", "delete")

    def test_acesso_negado(self):
        """Deve lancar PermissionError para acesso nao autorizado."""
        with pytest.raises(PermissionError):
            self.ac.require_permission("visitante_01", "delete")

    def test_acesso_permitido(self):
        """Nao deve lancar excecao para acesso autorizado."""
        self.ac.require_permission("admin", "delete")

    def test_usuario_inexistente(self):
        """Usuario inexistente nao deve ter permissoes."""
        perms = self.ac.get_permissions("usuario_fake")
        assert perms == []

    def test_audit_log(self):
        """Log de auditoria deve ter campos obrigatorios."""
        entry = self.ac.audit_log("admin", "read", "dados_pacientes")
        assert "timestamp" in entry
        assert entry["user_id"] == "admin"
        assert entry["role"] == "admin"
        assert entry["action"] == "read"
        assert entry["resource"] == "dados_pacientes"
        assert entry["authorized"] is True

    def test_audit_log_nao_autorizado(self):
        """Log de auditoria deve registrar acesso nao autorizado."""
        entry = self.ac.audit_log("visitante_01", "delete", "dados_pacientes")
        assert entry["authorized"] is False
