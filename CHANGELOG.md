# Changelog

## [Unreleased]

### Novas funcionalidades
- **Banco de dados nas respostas de Vacuum / Saúde e Saúde do BD**: `database_name` em vacuum, sessões idle in transaction, bloat de índice, funções, consumidores de conexão (agrupados também por banco), índices não usados, I/O de tabelas, saúde do schema, buffer cache, hot spots e tendências de queries
- Migration `d2e3f4a5b6c7`: coluna `database_name` em `query_stats_snapshot`

## [0.2.2] — 2026-11-26

### Correções
- **SQL Server — todas as instâncias em ERR até reiniciar**: o pymssql 2.4.1 tinha um deadlock (trava não reentrante no tratamento de conexão morta) que congelava toda coleta SQL Server do processo depois de uma oscilação de rede. Dependência atualizada para `pymssql>=2.4.6`, que corrige o problema
- **Saúde do BD — tendências de queries**: a consulta era inválida e o painel dava erro em todos os engines
- **Vacuum / Saúde (PostgreSQL)**: as duas consultas rodavam ao mesmo tempo na mesma conexão e a tela dava erro sempre; agora rodam em sequência
- **Saúde do BD — I/O por arquivo (SQL Server)**: usava colunas inexistentes de `sys.dm_io_virtual_file_stats`

### Documentação
- Specs de tela atualizadas (navegação, Particionamento e compressão, Bloqueios, Saúde do BD, Memória, Queries lentas, Login, manutenção) e decisões de arquitetura (driver SQL Server, watchdog do pool)

## [0.2.1] — 2026-11-25

### Correções
- **SQL Server — instância presa em ERR**: quando a rede cai em silêncio (sem RST/FIN), as threads do pymssql travavam e o pool da instância saturava, deixando-a em ERR até reiniciar o backend. Um watchdog agora recria o pool quando todas as threads ficam presas por mais de `INSTANCE_EXECUTOR_STUCK_THRESHOLD_SECONDS` (padrão 90s)
- **Endpoints de saúde**: `/tempdb` em instâncias PostgreSQL e 7 endpoints em SQL Server (bloat, bgwriter, funções, replication slots, índices não usados, I/O de tabelas, vacuum) davam erro 502 em vez da resposta vazia
- **PostgreSQL — sugestões de índice ausente**: uma falha na consulta extra ao pg_qualstats descartava todas as sugestões já coletadas
- **Oracle — sugestões de índice ausente**: falhavam sempre por chamar uma função inexistente

### Manutenção
- Código do backend sem erros de lint e de formatação (ruff)

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
