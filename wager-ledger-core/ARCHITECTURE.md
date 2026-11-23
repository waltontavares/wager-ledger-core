# Arquitetura do Núcleo Financeiro (Wager Ledger)

## 1. Stack e Runtime
- **Runtime:** Bun 1.x com TypeScript em modo estrito (`strict: true`, `noUncheckedIndexedAccess: true`).
- **Framework:** NestJS estruturado sob Arquitetura Hexagonal (Ports & Adapters).
- **Infraestrutura Local:** Docker Compose com PostgreSQL 17 e MiniStack (SQS FIFO + DLQ).

---

## 2. Persistência e Controle de Concorrência (MikroORM)
Escolhemos o **MikroORM** (com driver `@mikro-orm/postgresql`) pelos seguintes motivos técnicos:
1. **Unit of Work Explícito:** Gerencia mudanças em memória e executa o flush atômico ao final do bloco `em.transactional(async (em) => { ... })`.
2. **Controle Fino de Locks:** Suporte nativo e tipado a `LockMode.PESSIMISTIC_WRITE` (`SELECT ... FOR UPDATE`), essencial para serializar débitos/créditos concorrentes na mesma carteira (`wallet_id`) sem race conditions ou saldo negativo.
3. **Identity Map:** Previne inconsistências de leitura repetida dentro da mesma transação financeira.

---

## 3. Modelagem e Mapeamento de `Money`
Para eliminar erros de precisão de ponto flutuante (IEEE 754), valores monetários **nunca** utilizam `number` ou `float`:
- **No Domínio (`Money` Value Object):** Representado internamente por `amountInCents: bigint` (minor units) acompanhado do código ISO-4217 (`currency: string`, ex.: `BRL`, `USD`). Toda operação valida igualdade de moeda e impede saldos negativos.
- **No Banco de Dados (PostgreSQL + MikroORM):**
  - Coluna `amount_cents`: `BIGINT` (mapeada via `BigIntType` do MikroORM), garantindo aritmética exata até 9 × 10¹⁸ centavos.
  - Coluna `currency`: `VARCHAR(3)` com constraint de moeda válida.

---

## 4. Identidade e Autenticação (`ProviderIdentityPort`)
Para não desviar tempo de engenharia do núcleo financeiro com a manutenção de um IdP externo (Keycloak/Zitadel) neste estágio, isolamos a autenticação atrás de uma porta de domínio:

```text
[HTTP Request]
      │  (Headers: X-Provider-Id, X-Internal-Secret)
      ▼
[ExplicitProviderIdentityGuard]  ──► Injeta `request.provider`
      │
      ▼
«interface» ProviderIdentityPort
      ▲
      ├── [HeaderProviderIdentityAdapter] (Ativo em Dev/Local)
      └── [JwtOidcProviderIdentityAdapter] (Evolução futura p/ Keycloak/Zitadel)


---

## 5. Modelo de Domínio, `Money` (`decimal.js`) e `payloadHash` Canônico

### 5.1 Precisão Monetária (`Money`)
- Utiliza `decimal.js` internamente com arredondamento `ROUND_HALF_EVEN` (Banker's Rounding) fixado em 2 casas decimais.
- `Money.fromContract(amount, currency)` valida estritamente a regex `/^\d+\.\d{2}$/`, rejeitando `number`, negativos, `NaN`, `Infinity` e notação científica (`1e2`).

### 5.2 Algoritmo Determinístico de `payloadHash`
Para detectar conflitos de idempotência (`IDEMPOTENCY_PAYLOAD_MISMATCH`) quando um provedor reutiliza o mesmo `(providerId, externalTransactionId)` com dados diferentes:
1. **Seleção de Campos de Negócio:** Apenas `{ providerId, externalTransactionId, walletId, kind, amount, currency, referenceExternalTransactionId, roundId, gameId }` entram no cálculo. Headers HTTP (`Idempotency-Key`, `X-Provider-Id`), timestamps de rede e IDs de mensagem SQS são descartados.
2. **Canonicalização JSON:** O objeto passa pela função recursiva `canonicalJsonStringify`, que remove chaves `undefined` e ordena todas as chaves lexicograficamente (UTF-16 code units).
3. **Digest Criptográfico:** Calcula-se `SHA-256` em codificação `UTF-8` sobre a string canônica resultante, produzindo um hash hexadecimal de 64 caracteres.

---

## 6. Semântica de Idempotência, Replay e `observedBalance`

Ao receber uma requisição identificada por `(providerId, idempotencyKey)`:

1. **Conflito de Payload (`409 Conflict`):** Se a chave já existir e o `payloadHash` canônico diferir da requisição original, a operação é rejeitada com `IDEMPOTENCY_PAYLOAD_MISMATCH` (`HTTP 409`), protegendo o ledger contra mutação acidental ou maliciosa de parâmetros.
2. **Replay Idempotente (`idempotentReplay: true`):** Se o `payloadHash` for idêntico, nenhum efeito colateral é reexecutado e o sistema retorna o estado persistido da transação junto ao `observedBalance`:
   - **Transações efetivadas com lançamento (`PROCESSED` para `OPENING > 0`, `BET`, `WIN`, `REFUND`, `ROLLBACK`):** Retorna o `balanceAfter` exato registrado no `WalletLedgerEntry` imutável no instante em que a transação ocorreu (mesmo que apostas posteriores já tenham alterado o saldo atual da carteira).
   - **Transações sem lançamento no ledger (`LOSS`, `REJECTED`, `PENDING`, `PENDING_REFERENCE`):** Retorna o saldo corrente da carteira no momento da consulta, pois não houve mutação de saldo vinculada à transação.

---

## 7. Resolução de Referências Fora de Ordem (`PENDING_REFERENCE`)

Em sistemas distribuídos assíncronos (SQS / webhooks de provedores), um `REFUND` ou `ROLLBACK` pode chegar antes da transação `BET`/`WIN` original:

1. **Estado Intermediário e Evento:** Ao detectar que `referenceExternalTransactionId` ainda não existe, a transação é persistida como `PENDING_REFERENCE` e emite `WagerTransactionPendingReference` na Outbox.
2. **Backoff Exponencial com Full Jitter:**
   - **Delay base:** `2s` (`PENDING_REF_BASE_DELAY_MS`), dobrando a cada tentativa (`2s -> 4s -> 8s -> 16s -> 32s`) com teto de `60s` (`PENDING_REF_MAX_DELAY_MS`) e *jitter* aleatório entre `50%` e `100%` para evitar *thundering herd* no PostgreSQL.
   - **Reprocessamento Unificado:** O worker agendado (`ReplayPendingReferencesUseCase`) invoca exatamente o mesmo `ProcessWagerTransactionUseCase` (`isReaperReplay: true`), reutilizando o lock pessimista por `walletId` e todas as regras de negócio.
3. **Limites (Max Attempts & TTL) e Rejeição Terminal:**
   - **Limite padrão:** `5 tentativas` (`PENDING_REF_MAX_ATTEMPTS`) ou janela máxima de `5 minutos` (`PENDING_REF_TTL_MS = 300000`), configuráveis via ambiente.
   - **Exaustão:** Se a referência não aparecer dentro do limite/TTL, a transação transiciona para o estado terminal `REJECTED` com `failureCode = REFERENCE_NOT_FOUND` e publica `WagerTransactionRejected` na Outbox.

---

## 8. Outbox Publisher Concorrente (`FOR UPDATE SKIP LOCKED`) e Idempotência Downstream

Para suportar múltiplas réplicas horizontais da aplicação publicando eventos simultaneamente sem race conditions:

1. **Particionamento Dinâmico via `FOR UPDATE SKIP LOCKED`:** Cada réplica executa a varredura dentro de uma transação SQL usando `SELECT ... WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= NOW()) ORDER BY next_attempt_at ASC, occurred_at ASC LIMIT 50 FOR UPDATE SKIP LOCKED`. As linhas já bloqueadas por uma instância são ignoradas instantaneamente pelas demais réplicas.
2. **Idempotência Ponta a Ponta (`eventId`):**
   - Cada evento recebe um `eventId` (UUIDv4) imutável gerado no momento da gravação na Outbox.
   - No SQS FIFO, `eventId` é enviado como `MessageDeduplicationId` e atributo de mensagem, garantindo entrega *at-least-once* com deduplicação nativa no broker e chave natural de idempotência para consumidores downstream.
3. **Resiliência com Backoff Exponencial:** Em caso de indisponibilidade do broker, o publisher incrementa `attempts` e calcula `next_attempt_at` com backoff exponencial + jitter (`1s -> 2s -> 4s ...` até `60s`), liberando o lock da linha sem bloquear outras mensagens.

---

## 9. Consumidor SQS FIFO, Política de DLQ e Graceful Shutdown

### 9.1 Tratamento Diferenciado de Falhas no Consumidor (`wager-transactions.fifo`)
- **Erro de Negócio Rejeitado (`status = REJECTED`, ex.: `INSUFFICIENT_FUNDS`, `ROLLBACK_WOULD_OVERDRAFT`):** O estado terminal e o evento `WagerTransactionRejected` são persistidos no PostgreSQL junto com a `InboxMessage`. Após o `COMMIT`, o consumidor executa `DeleteMessage` (ack).
- **Erro Permanente / Poison Message (`MALFORMED_JSON` ou contrato inválido pré-transação):** Como repetir o processamento nunca terá sucesso e travaria o `MessageGroupId` da fila FIFO, o consumidor encaminha um envelope estruturado para `wager-transactions-dlq.fifo` e executa `DeleteMessage` na fila principal.
- **Erro Transitório (Indisponibilidade de DB/rede ou timeout de lock):** Não executa `DeleteMessage`. Ajusta `VisibilityTimeout` com backoff exponencial (`2s -> 4s -> 8s...`) para retentativa. Ao atingir `SQS_MAX_RECEIVE_COUNT` (padrão `3`), encaminha para `wager-transactions-dlq.fifo` (em conjunto com a `RedrivePolicy` configurada na fila SQS).

### 9.2 Graceful Shutdown (`SIGTERM`)
1. Ao receber `SIGTERM`, o NestJS dispara `beforeApplicationShutdown`: o flag `shuttingDown` interrompe imediatamente novas leituras na fila e limpa os timers de Outbox/Reaper.
2. Caso uma resposta de *long polling* chegue durante o desligamento, o worker devolve a visibilidade (`VisibilityTimeout = 0`) sem iniciar processamento.
3. O processo aguarda a conclusão das transações em voo (`inFlightTasks`) por até `SHUTDOWN_GRACE_PERIOD_MS` (padrão `15s`). Se o tempo estourar, libera a visibilidade das mensagens remanescentes para reentrega imediata por outra réplica e encerra o `SQSClient` e o pool do MikroORM.
