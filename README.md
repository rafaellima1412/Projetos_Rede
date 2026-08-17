# Projetos_Rede — Laboratório TR-069/TR-181 (GenieACS)

Ambiente de estudo/laboratório para entender, na prática, como funciona o gerenciamento remoto de CPEs (roteadores, ONTs, gateways) via **TR-069/CWMP**, usando o **TR-181** como modelo de dados — evoluindo até uma arquitetura de eventos capaz de responder, por CPE, se uma instalação foi realmente bem-sucedida.

## Objetivo final

Responder, de forma automatizada, ao seguinte funil por cliente:

```
Instalação concluída → CPE provisionado → Teste de conectividade OK →
Sem degradação relevante → Sem nova visita técnica → Sem chamado em 30 dias
→ ✅ Instalação bem-sucedida
```

## O que este projeto demonstra

- Como um ACS (Auto Configuration Server) gerencia CPEs remotamente via TR-069
- Como o modelo de dados TR-181 estrutura parâmetros (estatísticas, diagnósticos, Wi-Fi, sinal óptico, etc)
- Como escalar um ACS horizontalmente em cluster — e a pegadinha real disso (sessão CWMP presa ao processo, exige sticky session)
- Como desenhar um pipeline orientado a eventos: **Event Ingestor → Fila de eventos → Consumers independentes**, cada um no seu ritmo e com seu propósito
- Como transformar eventos técnicos brutos num funil de negócio (sucesso de instalação)

## Arquitetura

![Arquitetura atual do projeto](arquitetura_atual.svg)

*(Diagrama gerado a partir do que já foi validado ponta a ponta em Ago/2026 — substitui os diagramas conceituais anteriores, que mostravam a arquitetura pretendida para 10 mil CPEs, não o estado real do lab.)*

**Status real de cada peça:**

| Peça | Status |
|---|---|
| Nginx com sticky session (`ip_hash`) na porta CWMP | ✅ Validado |
| Cluster GenieACS (2 instâncias, MongoDB compartilhado) | ✅ Validado |
| Data model estendido (parâmetros TR-181 customizados) | ✅ Validado |
| Event Ingestor (`publish_kafka.js`) publicando direto no Kafka | ✅ Validado |
| Consumer Node.js → PostgreSQL + Redis | ✅ Validado |
| Funil de sucesso de instalação (`funil_sucesso_instalacao`) | ✅ Schema pronto, alimentado pelo Consumer |
| Workflow n8n com Kafka Trigger (alertas) | 🔶 Pendente |
| Integração com sistema real de chamados/visitas | 🔶 Pendente (hoje simulado em `tickets_simulados`) |

```
CPEs simulados (genieacs-sim, 100 devices)
        ↓
Nginx (load balancer, sticky session via ip_hash na porta CWMP)
        ↓
Cluster GenieACS (2 instâncias, MongoDB compartilhado)
        ↓
Event Ingestor (extension publish_kafka.js — publica direto via kafkajs)
        ↓
Kafka (tópico device-events)
        ↓
   ┌─────────────────────────┬──────────────────────────────┐
   ↓                         ↓
Consumer (Node.js)      n8n — Kafka Trigger (pendente)
   ├── PostgreSQL            └── Alertas de degradação
   │    ├── device_events
   │    ├── device_lifecycle
   │    └── funil_sucesso_instalacao (view)
   └── Redis (cache do último estado, TTL 1h)
```

### Decisões técnicas e por quê

**Event Ingestor separado do Consumer.** A extension do GenieACS (`publish_kafka.js`) só tem uma responsabilidade: pegar o evento bruto do Inform e publicar no Kafka, já formatado. Ela não sabe nada sobre Postgres, Redis ou regras de negócio — isso é papel do Consumer, mais à frente na cadeia.

**Por que o n8n saiu do caminho de ingestão.** Inicialmente o GenieACS notificava o n8n via webhook, que publicava no Kafka. Com carga de eventos (múltiplos CPEs enviando Inform quase simultaneamente), esse hop HTTP extra virou gargalo desnecessário. A extension agora publica **direto** no Kafka via `kafkajs`. O n8n ficou reservado só para consumo de baixo volume — alertas e automações — onde o overhead de um workflow visual não é problema.

**Múltiplos consumers independentes no mesmo tópico.** O Consumer em Node.js (grupo `consumer-postgres-redis`) e o futuro workflow do n8n (via Kafka Trigger) leem o **mesmo** tópico `device-events`, cada um com seu próprio consumer group — um não bloqueia nem depende do outro.

**Cada destino de dado com um propósito diferente:**
| Destino | O que grava | Por quê |
|---|---|---|
| PostgreSQL | Todo evento, sempre (`device_events`) + estado do funil (`device_lifecycle`) | Histórico completo, auditável, consultas históricas |
| Redis | Só o último estado de cada CPE, com expiração de 1h | Cache rápido, não serve pra histórico |
| n8n | Só quando degradação é detectada | Mantém baixo volume, evita gargalo |

**Sticky session no load balancer.** Uma sessão CWMP é uma sequência de múltiplas trocas HTTP que precisa ser atendida pela **mesma instância** do GenieACS do início ao fim — o contexto fica vinculado ao processo, não é compartilhado entre instâncias via MongoDB. Sem `ip_hash` na porta CWMP, ocorre o erro `Invalid session`.

## O funil de sucesso de instalação

A tabela `device_lifecycle` e a view `funil_sucesso_instalacao` (em `schema.sql`) rastreiam, por CPE:

1. **Instalação concluída** e **2. CPE provisionado** — alimentados automaticamente pelo Consumer a partir dos eventos do Kafka
2. **Teste de conectividade OK** — calculado a partir do status da conexão WAN (TR-181)
3. **Sem degradação relevante** — score de qualidade calculado a partir de sinal óptico, uptime, status WAN e Wi-Fi
4. **Sem nova visita técnica** e **6. Sem chamado em 30 dias** — hoje **simulados/manuais** na tabela `tickets_simulados` (o projeto ainda não integra um sistema real de ticketing/CRM — a estrutura já está pronta pra isso no futuro)

## Como rodar

Pré-requisitos: Docker e Docker Compose.

```bash
docker compose up -d --build
docker compose --profile testing up -d   # sobe os CPEs simulados
```

Serviços disponíveis:

| Serviço | URL/Porta | O que é |
|---|---|---|
| GenieACS UI | http://localhost:3000 | Interface de administração (via Nginx) |
| GenieACS NBI | http://localhost:7557 | API REST pra scripts/automação |
| GenieACS CWMP | :7547 | Porta onde os CPEs se conectam |
| Kafka UI | http://localhost:8081 | Visualizar tópicos/mensagens do Kafka |
| n8n | http://localhost:5678 | Automações e alertas |
| MongoDB | :27017 (localhost) | Banco do GenieACS (ex: pra conectar via Compass) |
| PostgreSQL | :5432 (localhost) | Histórico de eventos e funil de sucesso |

## Estrutura do repositório

| Arquivo/Pasta | Descrição |
|---|---|
| `docker-compose.yml` | Orquestração de todos os serviços |
| `nginx.conf` | Load balancer com sticky session (`ip_hash`) na porta CWMP |
| `data_model_extended.csv` | Data model do `genieacs-sim` estendido com parâmetros TR-181 customizados |
| `extensions/publish_kafka.js` | Event Ingestor — publica eventos direto no Kafka |
| `notify_provision.js` | Provisioning script que dispara a extension a cada Inform |
| `schema.sql` | Schema do Postgres: histórico, funil de sucesso e tickets simulados |
| `consumer/` | Consumer Node.js: Kafka → Postgres + Redis |
| `coletor_cpe.py` | Script Python de exemplo pra consultar dados de um CPE via NBI |
| `arquitetura_atual.svg` | Diagrama da arquitetura validada (este README) |

## Próximos passos

- [ ] Workflow n8n dedicado a alertas (Kafka Trigger → filtro de degradação → notificação)
- [ ] Integrar sistema real de chamados/visitas técnicas (substituindo `tickets_simulados`)
- [ ] Testes de carga com volume maior de CPEs simulados
- [ ] Dashboard consumindo a view `funil_sucesso_instalacao`

## Contexto

Projeto desenvolvido como estudo prático de TR-069/TR-181, evoluindo de um script simples de coleta de dados até uma arquitetura orientada a eventos para acompanhar o sucesso de instalações em escala.

