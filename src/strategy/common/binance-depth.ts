import NodeWebSocket from "ws";
import { computeDepthStats, type DepthImbalance } from "../../utils/depth";

const WebSocketCtor: typeof globalThis.WebSocket =
  typeof globalThis.WebSocket !== "undefined"
    ? globalThis.WebSocket
    : ((NodeWebSocket as unknown) as typeof globalThis.WebSocket);

const DEFAULT_BASE_URL = "wss://fstream.binance.com/ws";

export interface BinanceDepthSnapshot {
  symbol: string;
  buySum: number;
  sellSum: number;
  skipBuySide: boolean;
  skipSellSide: boolean;
  imbalance: DepthImbalance;
  updatedAt: number;
}

export class BinanceDepthTracker {
  private ws: WebSocket | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectDelayMs = 3000;
  private stopped = false;
  private snapshot: BinanceDepthSnapshot | null = null;
  private listeners = new Set<(snapshot: BinanceDepthSnapshot) => void>();

  constructor(
    private readonly symbol: string,
    private readonly options?: {
      baseUrl?: string;
      levels?: number;
      ratio?: number;
      logger?: (context: string, error: unknown) => void;
    }
  ) {}

  start(): void {
    this.stopped = false;
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      try {
        this.ws.close();
      } catch {
        // Ignore close errors
      }
      this.ws = null;
    }
  }

  onUpdate(handler: (snapshot: BinanceDepthSnapshot) => void): void {
    this.listeners.add(handler);
  }

  offUpdate(handler: (snapshot: BinanceDepthSnapshot) => void): void {
    this.listeners.delete(handler);
  }

  getSnapshot(): BinanceDepthSnapshot | null {
    return this.snapshot ? { ...this.snapshot } : null;
  }

  private connect(): void {
    if (this.ws || this.stopped) return;
    const url = this.buildUrl();
    this.ws = new WebSocketCtor(url);

    const handleOpen = () => {
      this.reconnectDelayMs = 3000;
    };

    const handleClose = () => {
      this.ws = null;
      if (!this.stopped) {
        this.scheduleReconnect();
      }
    };

    const handleError = (error: unknown) => {
      this.options?.logger?.("binanceDepth", error);
    };

    const handleMessage = (event: { data: unknown }) => {
      this.handlePayload(event.data);
    };

    const handlePing = (data: unknown) => {
      if (this.ws && "pong" in this.ws && typeof this.ws.pong === "function") {
        this.ws.pong(data as any);
      }
    };

    if ("addEventListener" in this.ws && typeof this.ws.addEventListener === "function") {
      this.ws.addEventListener("open", handleOpen);
      this.ws.addEventListener("message", handleMessage as any);
      this.ws.addEventListener("close", handleClose);
      this.ws.addEventListener("error", handleError as any);
      this.ws.addEventListener("ping", handlePing as any);
    } else if ("on" in this.ws && typeof (this.ws as any).on === "function") {
      const nodeSocket = this.ws as any;
      nodeSocket.on("open", handleOpen);
      nodeSocket.on("message", (data: unknown) => handleMessage({ data }));
      nodeSocket.on("close", handleClose);
      nodeSocket.on("error", handleError);
      nodeSocket.on("ping", handlePing);
    } else {
      (this.ws as any).onopen = handleOpen;
      (this.ws as any).onmessage = handleMessage;
      (this.ws as any).onclose = handleClose;
      (this.ws as any).onerror = handleError;
    }
  }

  private buildUrl(): string {
    const base = this.options?.baseUrl ?? DEFAULT_BASE_URL;
    const stream = `${this.symbol.toLowerCase()}@depth10@100ms`;
    return `${base}/${stream}`;
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer || this.stopped) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.reconnectDelayMs = Math.min(this.reconnectDelayMs * 2, 60_000);
      this.connect();
    }, this.reconnectDelayMs);
  }

  private handlePayload(data: unknown): void {
    const payload = this.parsePayload(data);
    if (!payload) return;
    const bids = Array.isArray(payload.b) ? payload.b : [];
    const asks = Array.isArray(payload.a) ? payload.a : [];
    const depth = {
      lastUpdateId: Number(payload.u ?? Date.now()),
      bids,
      asks,
    };
    const levels = this.options?.levels ?? 10;
    const ratio = this.options?.ratio ?? 3;
    const stats = computeDepthStats(depth, levels, ratio);
    this.snapshot = {
      symbol: this.symbol,
      buySum: stats.buySum,
      sellSum: stats.sellSum,
      skipBuySide: stats.skipBuySide,
      skipSellSide: stats.skipSellSide,
      imbalance: stats.imbalance,
      updatedAt: Date.now(),
    };
    for (const listener of this.listeners) {
      listener({ ...this.snapshot });
    }
  }

  private parsePayload(data: unknown): { b?: [string, string][]; a?: [string, string][]; u?: number } | null {
    try {
      const text = typeof data === "string" ? data : Buffer.isBuffer(data) ? data.toString("utf-8") : null;
      if (!text) return null;
      const parsed = JSON.parse(text);
      if (!parsed || typeof parsed !== "object") return null;
      return parsed as { b?: [string, string][]; a?: [string, string][]; u?: number };
    } catch {
      return null;
    }
  }
}

