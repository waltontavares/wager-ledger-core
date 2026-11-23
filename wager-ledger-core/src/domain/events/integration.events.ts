import { randomUUID } from 'node:crypto';

export interface IntegrationEventEnvelope<TData = Record<string, unknown>> {
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId: string;
  occurredAt: string;
  version: number;
  data: TData;
}

export class IntegrationEvent<TData = Record<string, unknown>> {
  public readonly eventId: string;
  public readonly eventType: string;
  public readonly aggregateId: string;
  public readonly correlationId: string;
  public readonly causationId: string;
  public readonly occurredAt: string;
  public readonly version: number;
  public readonly data: TData;

  private constructor(envelope: IntegrationEventEnvelope<TData>) {
    this.eventId = envelope.eventId;
    this.eventType = envelope.eventType;
    this.aggregateId = envelope.aggregateId;
    this.correlationId = envelope.correlationId;
    this.causationId = envelope.causationId;
    this.occurredAt = envelope.occurredAt;
    this.version = envelope.version;
    this.data = Object.freeze({ ...envelope.data });
    Object.freeze(this);
  }

  static create<TData = Record<string, unknown>>(params: {
    eventId?: string;
    eventType: string;
    aggregateId: string;
    correlationId: string;
    causationId: string;
    occurredAt?: string;
    version?: number;
    data: TData;
  }): IntegrationEvent<TData> {
    return new IntegrationEvent({
      eventId: params.eventId ?? randomUUID(),
      eventType: params.eventType,
      aggregateId: params.aggregateId,
      correlationId: params.correlationId,
      causationId: params.causationId,
      occurredAt: params.occurredAt ?? new Date().toISOString(),
      version: params.version ?? 1,
      data: params.data,
    });
  }

  static fromEnvelope<TData = Record<string, unknown>>(
    envelope: IntegrationEventEnvelope<TData>,
  ): IntegrationEvent<TData> {
    return new IntegrationEvent(envelope);
  }

  toJSON(): IntegrationEventEnvelope<TData> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      causationId: this.causationId,
      occurredAt: this.occurredAt,
      version: this.version,
      data: { ...this.data },
    };
  }
}
