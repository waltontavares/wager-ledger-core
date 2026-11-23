import { Injectable } from '@nestjs/common';
import { LoggerPort, MetricsPort } from '../../application/ports/application.ports.js';

const ALLOWED_LOG_FIELDS = new Set<string>([
  'correlationId',
  'messageId',
  'transactionId',
  'txId',
  'walletId',
  'providerId',
  'kind',
  'status',
  'code',
  'failureCode',
  'signal',
  'attempts',
  'receiveCount',
  'inFlightCount',
  'remainingCount',
  'visibilityTimeoutSec',
  'outboxId',
  'eventId',
  'consumer',
  'currency',
  'checkedEntries',
]);

function sanitizeLogMeta(meta?: Record<string, unknown>): Record<string, unknown> {
  if (!meta) return {};
  const clean: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(meta)) {
    if (ALLOWED_LOG_FIELDS.has(key) && value !== undefined) {
      clean[key === 'txId' ? 'transactionId' : key] = value;
    }
  }
  return clean;
}

@Injectable()
export class StructuredJsonLogger implements LoggerPort {
  info(message: string, meta?: Record<string, unknown>): void {
    this.write('INFO', message, meta);
  }

  warn(message: string, meta?: Record<string, unknown>): void {
    this.write('WARN', message, meta);
  }

  error(message: string, error?: unknown, meta?: Record<string, unknown>): void {
    const errInfo =
      error instanceof Error
        ? { errorName: error.name, errorCode: (error as { code?: string }).code ?? 'UNKNOWN' }
        : error
          ? { errorName: 'UnknownError' }
          : {};
    this.write('ERROR', message, { ...meta, ...errInfo });
  }

  private write(level: 'INFO' | 'WARN' | 'ERROR', message: string, meta?: Record<string, unknown>): void {
    const entry = {
      timestamp: new Date().toISOString(),
      level,
      service: 'wager-ledger-core',
      message,
      ...sanitizeLogMeta(meta),
    };
    const serialized = JSON.stringify(entry);
    if (level === 'ERROR') {
      console.error(serialized);
    } else if (level === 'WARN') {
      console.warn(serialized);
    } else {
      console.log(serialized);
    }
  }
}

interface HistogramBucket {
  count: number;
  sum: number;
  min: number;
  max: number;
}

@Injectable()
export class PrometheusMetricsRegistry implements MetricsPort {
  private readonly counters = new Map<string, number>();
  private readonly histograms = new Map<string, HistogramBucket>();

  incrementCounter(name: string, tags?: Record<string, string>): void {
    const key = this.formatKey(name, tags);
    this.counters.set(key, (this.counters.get(key) ?? 0) + 1);
  }

  observeHistogram(name: string, valueMs: number, tags?: Record<string, string>): void {
    const key = this.formatKey(name, tags);
    const current = this.histograms.get(key);
    if (!current) {
      this.histograms.set(key, {
        count: 1,
        sum: valueMs,
        min: valueMs,
        max: valueMs,
      });
    } else {
      current.count += 1;
      current.sum += valueMs;
      current.min = Math.min(current.min, valueMs);
      current.max = Math.max(current.max, valueMs);
    }
  }

  renderPrometheusText(gauges: Record<string, number> = {}): string {
    const lines: Array<string> = [];

    for (const [key, val] of this.counters.entries()) {
      lines.push(`${key} ${val}`);
    }

    for (const [key, bucket] of this.histograms.entries()) {
      const baseName = key.includes('{') ? key.slice(0, key.indexOf('{')) : key;
      const labels = key.includes('{') ? key.slice(key.indexOf('{')) : '';
      lines.push(`${baseName}_count${labels} ${bucket.count}`);
      lines.push(`${baseName}_sum${labels} ${bucket.sum.toFixed(2)}`);
      lines.push(`${baseName}_avg${labels} ${(bucket.sum / bucket.count).toFixed(2)}`);
      lines.push(`${baseName}_max${labels} ${bucket.max.toFixed(2)}`);
    }

    for (const [name, val] of Object.entries(gauges)) {
      lines.push(`${name} ${val}`);
    }

    return lines.join('\n') + '\n';
  }

  private formatKey(name: string, tags?: Record<string, string>): string {
    const metricName = name.replace(/[^a-zA-Z0-9_]/g, '_');
    if (!tags || Object.keys(tags).length === 0) {
      return metricName;
    }
    const labelStr = Object.entries(tags)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}="${String(v).replace(/"/g, '\\"')}"`)
      .join(',');
    return `${metricName}{${labelStr}}`;
  }
}
