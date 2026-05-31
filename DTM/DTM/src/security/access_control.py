"""
Controle de acesso baseado em papéis (RBAC) para o pipeline de dados.

Define permissões por perfil de usuário, garantindo que apenas
usuários autorizados possam ler, escrever ou deletar dados.
"""

import logging
from typing import List

logger = logging.getLogger(__name__)


class AccessController:
    ROLES = {
        "admin": {
            "permissions": ["read", "write", "delete", "mask", "export"],
            "description": "Acesso total ao pipeline e dados",
        },
        "analista": {
            "permissions": ["read", "write"],
            "description": "Leitura e escrita de dados processados",
        },
        "cientista_dados": {
            "permissions": ["read", "write", "export"],
            "description": "Leitura, escrita e exportação para análise",
        },
        "visitante": {
            "permissions": ["read"],
            "description": "Apenas visualização de dados mascarados",
        },
    }

    def __init__(self):
        self.users = {
            "admin": "admin",
            "analista_01": "analista",
            "cientista_01": "cientista_dados",
            "visitante_01": "visitante",
        }

    def get_user_role(self, user_id: str) -> str:
        return self.users.get(user_id, "")

    def get_permissions(self, user_id: str) -> List[str]:
        role = self.get_user_role(user_id)
        return self.ROLES.get(role, {}).get("permissions", [])

    def has_permission(self, user_id: str, permission: str) -> bool:
        return permission in self.get_permissions(user_id)

    def require_permission(self, user_id: str, permission: str) -> None:
        if not self.has_permission(user_id, permission):
            logger.warning("ACESSO NEGADO: user=%s permission=%s", user_id, permission)
            raise PermissionError(f"Usuário '{user_id}' não possui permissão '{permission}'")
        logger.info("Acesso concedido: user=%s permission=%s", user_id, permission)

    def audit_log(self, user_id: str, action: str, resource: str) -> dict:
        """Gera registro de auditoria para conformidade LGPD."""
        import datetime

        entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "user_id": user_id,
            "role": self.get_user_role(user_id),
            "action": action,
            "resource": resource,
            "authorized": self.has_permission(user_id, action),
        }
        logger.info("AUDIT: %s", entry)
        return entry
