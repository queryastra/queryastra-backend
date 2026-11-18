# Changelog

## [0.2.0] — 2026-11-18

### Novas funcionalidades
- **Central de notificações**: notificações em tempo real para alertas, consultas lentas e eventos de saúde
- **Gestão de equipes**: criação e administração de times com permissões por instância
- **Escopo e canais de alerta**: regras de alerta com escopo por banco/schema/objeto e entrega via e-mail, webhook ou Slack
- **Histórico de queries e deadlocks**: navegação e análise de execuções passadas com gráficos de tendência
- **Anotações**: marcadores manuais no timeline para correlacionar eventos com mudanças de código ou deploys
- **Visão geral (overview)**: painel consolidado com saúde, consultas lentas e atividade recente por instância
- **Exportação PDF**: relatórios de diagnóstico exportáveis em PDF
- **E-mail de boas-vindas**: enviado automaticamente ao iniciar trial
- **Excluir conta**: endpoint `DELETE /settings/account` para remoção de dados pelo usuário

### Correções
- Portal de billing roteado pelo proxy `queryastra.com` para evitar exposição de chaves Stripe no frontend
- WebSocket handler `/live` agora detecta desconexão do cliente e encerra a corrotina imediatamente
- Supervisor de histórico de planos não crashava mais em instâncias sem dados prévios
- Seletor de banco de dados em instâncias com muitos databases não travava mais o backend ao sair
- `/databases` retorna 504 após timeout de 15s em vez de ficar pendente indefinidamente
- Cache de `query_samples` consultado antes de executar query ao vivo em `/databases`

## [0.1.3] — 2026-10-26

### Novas funcionalidades
- Sugestões de índices ausentes para MySQL, Oracle e Db2
- Manutenção: Saúde de índices, Estatísticas, Memory Grants, Compressão, Partições, detecção de anomalias sazonais
- Worker de expiração de trial com e-mail automático
- Auto-update: verifica GitHub Releases e instala nova versão (admin only)
- Endpoint de portal de billing Stripe
- Enforcement de downgrade de plano com pausa de instâncias excedentes

### Correções
- 3 bugs de compatibilidade PostgreSQL 17+/18

## [0.1.2] — 2026-10-13

### Novas funcionalidades
- Suporte a MySQL, MariaDB, CockroachDB e Oracle
- Telas de saúde de índices e estatísticas

## [0.1.1] — 2026-10-07

### Novas funcionalidades
- Faturamento: planos, trials, integração Stripe
- Auditoria de ações administrativas

## [0.1.0] — 2026-09-30

### Lançamento inicial
- Monitoramento agentless de PostgreSQL e SQL Server
- Dashboard de saúde, consultas lentas, deadlocks, waits, sessões
- Explain Plan e análise de execução
