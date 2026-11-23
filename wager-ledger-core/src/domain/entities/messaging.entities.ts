import {
  IntegrationEvent,
  IntegrationEventEnvelope,
} from '../events/integration.events.js';
export type { IntegrationEventEnvelope };

export class InboxMessage {
  private constructor(
    public readonly consumerName: string,
    public readonly messageId: string,
    public readonly receivedAt: Date,
    private _processedAt: Date | null,
  ) {}

  static receive(consumerName: string, messageId: string): InboxMessage {
    return new InboxMessage(consumerName, messageId, new Date(), null);
  }

  static rehydrate(props: {
    consumerName: string;
    messageId: string;
    receivedAt: Date;
    processedAt: Date | null;
  }): InboxMessage {
    return new InboxMessage(
      props.consumerName,
      props.messageId,
      props.receivedAt,
      props.processedAt,
    );
  }

  get processedAt(): Date | null {
    return this._processedAt;
  }

  get isProcessed(): boolean {
    return this._processedAt !== null;
  }

  markProcessed(at: Date = new Date()): void {
    if (!this._processedAt) {
      this._processedAt = at;
    }
  }
}

export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly envelope: IntegrationEventEnvelope,
    private _attempts: number,
    private _nextAttemptAt: Date,
    private _publishedAt: Date | null,
    public readonly createdAt: Date,
  ) {}

  static create(event: IntegrationEvent): OutboxMessage {
    const serialized = event.toJSON();
    const now = new Date();
    return new OutboxMessage(
      serialized.eventId,
      Object.freeze(serialized),
      0,
      now,
      null,
      now,
    );
  }

  static rehydrate(props: {
    id: string;
    envelope: IntegrationEventEnvelope;
    attempts: number;
    nextAttemptAt: Date;
    publishedAt: Date | null;
    createdAt: Date;
  }): OutboxMessage {
    return new OutboxMessage(
      props.id,
      props.envelope,
      props.attempts,
      props.nextAttemptAt,
      props.publishedAt,
      props.createdAt,
    );
  }

  get attempts(): number {
    return this._attempts;
  }

  get nextAttemptAt(): Date {
    return this._nextAttemptAt;
  }

  get publishedAt(): Date | null {
    return this._publishedAt;
  }

  markPublished(at: Date = new Date()): void {
    this._publishedAt = at;
  }

  recordAttemptFailure(backoffMs: number): void {
    this._attempts += 1;
    this._nextAttemptAt = new Date(Date.now() + backoffMs);
  }
}
