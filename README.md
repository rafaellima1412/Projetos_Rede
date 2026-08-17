# Projetos_Rede

Pipeline de coleta e processamento de dados de CPEs via TR-069, usando GenieACS como ACS, com ingestão de eventos via Kafka e persistência para análise (funil de sucesso / FTT operacional).

## Objetivo

Coletar dados técnicos dos CPEs (status de conexão, sinal óptico, tráfego, firmware etc.) via TR-069 e transformar isso em indicadores operacionais — o principal sendo o **FTT (First Time Through) Operacional**, que cruza dados técnicos do CPE com eventos de processo (OS, chamados, ativação) pra medir se a jornada do cliente funcionou corretamente já na primeira tentativa, sem depender só do que a equipe registrou manualmente.

## Arquitetura

```
CPEs (ONU/Router)
   │  TR-069 (porta 7547)
   ▼
NGINX (LB) ──┬── GenieACS instância 1
             └── GenieACS instância 2
                       │
                       ▼
                  MongoDB (compartilhado)
                       ▲
                       │  polling NBI (delta por _lastInform)
                  ingestor  ← cursor de polling salvo no Redis
                       │
                       ▼
                Kafka (tópico device-events)
                       │
                       ▼
                  consumer
                   ├──► PostgreSQL (histórico / funil de sucesso)
                   ├──► Redis (cache do último estado de cada CPE)
                   └──► Kafka (tópico alertas-ftt, só quando detecta
                        problema: FTT quebrado, degradação, reincidência)
                                   │
                                   ▼
                        n8n (Kafka Trigger no tópico alertas-ftt)
                                   │
                                   ▼
                        Slack / e-mail / ticket no CRM
```

**Por que essa divisão:** o `ingestor` precisa aguentar volume alto (milhares de CPEs informando com frequência) — por isso é um serviço fino, sem overhead de motor de workflow. O n8n só entra depois que o volume já foi filtrado pelas regras de FTT, quando o que sobra são poucos eventos realmente relevantes por hora — aí sim ele é a ferramenta certa, porque orquestrar uma notificação/ação não exige alta vazão.

Diagramas visuais de referência: `arquitetura.png`, `arq2.png`, `arquitetura_10k_cpes.png`, `arquitetura_10k_cpes_v2.png`.

## Componentes

| Serviço | Função |
|---|---|
| `genieacs` / `genieacs-2` | ACS (Auto Configuration Server) TR-069, via GenieACS. Duas instâncias compartilhando o mesmo MongoDB, atrás do NGINX |
| `nginx` | Load balancer. Porta 7547 (CWMP, onde os CPEs falam com o ACS) usa `ip_hash` — necessário porque uma sessão CWMP é uma troca de várias mensagens que precisa ficar na mesma instância. Portas 7557 (NBI), 7567 (FS) e 3000 (UI) usam round-robin simples, por serem stateless |
| `mongo` | Banco de dados do GenieACS — árvore de parâmetros de cada CPE |
| `kafka` / `kafka-ui` | Fila de eventos (modo KRaft, sem Zookeeper). `kafka-ui` é só o painel web pra inspecionar tópicos e mensagens |
| `ingestor` | Serviço próprio (`./ingestor`), Node.js. Faz polling incremental na NBI do GenieACS (dispositivos com `_lastInform` mais recente que o último cursor salvo no Redis) e publica um evento por CPE no tópico `device-events`. É a camada de alto volume — sem motor de workflow, sem histórico de execução por evento |
| `n8n` | Orquestrador de alertas de baixo volume. Escuta o tópico `alertas-ftt` (via Kafka Trigger node, configurado na UI do n8n) e dispara ação — Slack, e-mail, ticket no CRM. Só recebe o que o `consumer` já filtrou como relevante, não o fluxo bruto de eventos |
| `consumer` | Serviço próprio (`./consumer`), Node.js. Consome `device-events`, grava histórico bruto em `device_events` e atualiza o funil por CPE em `device_lifecycle`. Pra eventos `inform`, compara com o último estado (Redis) pra detectar desconexão, reboot inesperado e sinal fora da faixa — quando detecta, loga como um `device_events` próprio e publica em `alertas-ftt` |
| `postgres` | Banco `genieacs_funil`. Tabelas: `device_lifecycle` (funil de sucesso por CPE, etapas 1-6), `device_events` (histórico bruto, inclui os alertas como tipo de evento), `simulated_tickets` (visitas/chamados simulados até integrar um sistema real). View `installation_success_funnel` calcula o status do funil em tempo real |
| `redis` | Cache da última telemetria conhecida de cada CPE — usado pelo `consumer` só pra comparar e detectar transições (desconexão, reboot), não é fonte de verdade |

## Scripts auxiliares

- `coletor_cpe.py`: coleta pontual e manual de dados de um CPE específico direto na NBI do GenieACS (TR-098/TR-181) — identificação, WAN, Wi-Fi, sinal óptico. Útil pra debug e inspeção ad-hoc, roda fora do pipeline de eventos
- `notify_provision.js`: hook de notificação de provisionamento
- `extensions/`: extensões customizadas do GenieACS

## Como rodar

```bash
docker compose up -d
```

Serviços expostos no host:
- `7547` — CWMP (CPEs se conectam aqui)
- `7557` — NBI (API administrativa)
- `7567` — FS (firmware)
- `3000` — GenieACS UI
- `5678` — n8n
- `8081` — Kafka UI
- `5432` — Postgres (só em `127.0.0.1`)

## Estado atual e limitações conhecidas

- **Credenciais hardcoded** (`GENIEACS_UI_JWT_SECRET=changeme`, usuário/senha padrão do Postgres) — trocar antes de qualquer ambiente exposto
- **MongoDB é instância única**, sem replicação — ponto único de falha e de capacidade. Não escala automaticamente ao adicionar mais instâncias de `genieacs-N`
- **Kafka roda com 1 broker e replication factor 1** — sem redundância. Além disso, o tópico `device-events` precisa de partições configuradas explicitamente pra permitir múltiplas réplicas do `consumer` processando em paralelo
- **`ip_hash` no NGINX depende de os CPEs terem IP de origem variado.** Se a base de clientes estiver atrás de CGNAT da própria operadora, `ip_hash` pode concentrar tráfego de forma desigual entre as instâncias — vale monitorar a contagem de dispositivos online por instância

## Roadmap

- [ ] Etapas 5 e 6 do funil (`last_technical_visit_at`, `last_support_ticket_at`) hoje só são preenchidas manualmente via `simulated_tickets` — integrar com sistema de chamados real quando existir
- [ ] Calibrar os pesos do `calculateQualityScore()` no `consumer` — hoje é uma heurística simples (100 se conectado e sinal na faixa, 40 pontos a menos se sinal degradado, 0 se desconectado), ainda não validada com dado real
- [ ] Configurar o workflow de alerta no n8n (Kafka Trigger node no tópico `alertas-ftt` → Slack/e-mail/CRM)
- [ ] MongoDB como replica set (3 nós) antes de crescer a base de CPEs
- [ ] Particionar tópico `device-events` e rodar múltiplas réplicas do `consumer`
- [ ] Adicionar coleta de contadores de tráfego (bytes/pacotes) e diagnósticos ativos (ping, download/upload) — hoje o `coletor_cpe.py`/`ingestor` cobrem identificação, WAN, Wi-Fi e sinal óptico, mas não contadores nem diagnóstico sob demanda
- [ ] Gatilho de scale-out do ACS baseado em métrica real (CPU/latência do `genieacs-cwmp`), não em contagem antecipada de CPEs
- [ ] Ajustar `POLL_INTERVAL_MS` do `ingestor` com base em volume real observado — 10s é ponto de partida, não valor validado em carga
- [ ] Calibrar `RX_POWER_MIN_DBM`/`RX_POWER_MAX_DBM` por vendor de ONU se a base tiver equipamentos heterogêneos — hoje é uma faixa única para todos os CPEs