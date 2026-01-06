import type { MakerPointsConfig } from "../config";
import type { ExchangeAdapter } from "../exchanges/adapter";
import type {
  AsterAccountSnapshot,
  AsterDepth,
  AsterOrder,
  AsterTicker,
} from "../exchanges/types";
import { formatPriceToString } from "../utils/math";
import { createTradeLog, type TradeLogEntry } from "../logging/trade-log";
import { extractMessage, isInsufficientBalanceError, isRateLimitError, isUnknownOrderError } from "../utils/errors";
import { isOrderActiveStatus } from "../utils/order-status";
import { getPosition, parseSymbolParts } from "../utils/strategy";
import type { PositionSnapshot } from "../utils/strategy";
import { computePositionPnl } from "../utils/pnl";
import { getMidOrLast, getTopPrices } from "../utils/price";
import {
  marketClose,
  placeOrder,
  unlockOperating,
} from "../core/order-coordinator";
import type { OrderLockMap, OrderPendingMap, OrderTimerMap } from "../core/order-coordinator";
import { makeOrderPlan } from "../core/lib/order-plan";
import { safeCancelOrder } from "../core/lib/orders";
import { RateLimitController } from "../core/lib/rate-limit";
import { StrategyEventEmitter } from "./common/event-emitter";
import { safeSubscribe, type LogHandler } from "./common/subscriptions";
import { SessionVolumeTracker } from "./common/session-volume";
import { BinanceDepthTracker, type BinanceDepthSnapshot } from "./common/binance-depth";
import { buildBpsTargets } from "./maker-points-logic";
import { t } from "../i18n";

interface DesiredOrder {
  side: "BUY" | "SELL";
  price: string;
  amount: number;
  reduceOnly: boolean;
}

export interface MakerPointsSnapshot {
  ready: boolean;
  symbol: string;
  topBid: number | null;
  topAsk: number | null;
  spread: number | null;
  priceDecimals: number;
  position: PositionSnapshot;
  pnl: number;
  accountUnrealized: number;
  sessionVolume: number;
  openOrders: AsterOrder[];
  desiredOrders: DesiredOrder[];
  tradeLog: TradeLogEntry[];
  lastUpdated: number | null;
  feedStatus: {
    account: boolean;
    orders: boolean;
    depth: boolean;
    ticker: boolean;
    binance: boolean;
  };
  binanceDepth: BinanceDepthSnapshot | null;
  quoteStatus: {
    closeOnly: boolean;
    skipBuy: boolean;
    skipSell: boolean;
  };
}

type MakerPointsEvent = "update";
type MakerPointsListener = (snapshot: MakerPointsSnapshot) => void;

const EPS = 1e-5;
const INSUFFICIENT_BALANCE_COOLDOWN_MS = 15_000;
const STOP_LOSS_COOLDOWN_MS = 10_000;

export class MakerPointsEngine {
  private accountSnapshot: AsterAccountSnapshot | null = null;
  private depthSnapshot: AsterDepth | null = null;
  private tickerSnapshot: AsterTicker | null = null;
  private openOrders: AsterOrder[] = [];

  private readonly locks: OrderLockMap = {};
  private readonly timers: OrderTimerMap = {};
  private readonly pending: OrderPendingMap = {};
  private readonly pendingCancelOrders = new Set<string>();

  private readonly tradeLog: ReturnType<typeof createTradeLog>;
  private readonly events = new StrategyEventEmitter<MakerPointsEvent, MakerPointsSnapshot>();
  private readonly sessionVolume = new SessionVolumeTracker();
  private readonly rateLimit: RateLimitController;
  private readonly binanceDepth: BinanceDepthTracker;

  private priceTick: number = 0.1;
  private qtyStep: number = 0.001;
  private precisionSync: Promise<void> | null = null;

  private timer: ReturnType<typeof setInterval> | null = null;
  private stopLossTimer: ReturnType<typeof setInterval> | null = null;
  private processing = false;
  private stopLossProcessing = false;
  private stopLossCooldownUntil = 0;
  private desiredOrders: DesiredOrder[] = [];
  private accountUnrealized = 0;
  private initialOrderSnapshotReady = false;
  private initialOrderResetDone = false;
  private lastDesiredSummary: string | null = null;
  private lastCloseOnly = false;
  private lastSkipBuy = false;
  private lastSkipSell = false;
  private lastQuoteBid1: number | null = null;
  private lastQuoteAsk1: number | null = null;

  private readinessLogged = {
    account: false,
    depth: false,
    ticker: false,
    orders: false,
  };
  private feedStatus = {
    account: false,
    depth: false,
    ticker: false,
    orders: false,
    binance: false,
  };
  private insufficientBalanceCooldownUntil = 0;
  private insufficientBalanceNotified = false;
  private lastInsufficientMessage: string | null = null;

  constructor(private readonly config: MakerPointsConfig, private readonly exchange: ExchangeAdapter) {
    this.tradeLog = createTradeLog(this.config.maxLogEntries);
    this.rateLimit = new RateLimitController(this.config.refreshIntervalMs, (type, detail) =>
      this.tradeLog.push(type, detail)
    );
    this.priceTick = Math.max(1e-9, this.config.priceTick);
    this.qtyStep = Math.max(1e-9, this.config.qtyStep);
    this.binanceDepth = new BinanceDepthTracker(resolveBinanceSymbol(this.config.symbol), {
      baseUrl: process.env.BINANCE_WS_URL,
      levels: 10,
      ratio: 3,
      logger: (context, error) => {
        this.tradeLog.push("warn", `Binance ${context} 异常: ${extractMessage(error)}`);
      },
    });
    this.binanceDepth.onUpdate(() => {
      this.feedStatus.binance = true;
      this.emitUpdate();
    });
    this.syncPrecision();
    this.bootstrap();
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.config.refreshIntervalMs);
    if (!this.stopLossTimer) {
      this.stopLossTimer = setInterval(() => {
        void this.checkStopLoss();
      }, Math.max(500, this.config.refreshIntervalMs));
    }
    this.binanceDepth.start();
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    if (this.stopLossTimer) {
      clearInterval(this.stopLossTimer);
      this.stopLossTimer = null;
    }
    this.binanceDepth.stop();
  }

  on(event: MakerPointsEvent, handler: MakerPointsListener): void {
    this.events.on(event, handler);
  }

  off(event: MakerPointsEvent, handler: MakerPointsListener): void {
    this.events.off(event, handler);
  }

  getSnapshot(): MakerPointsSnapshot {
    return this.buildSnapshot();
  }

  private bootstrap(): void {
    const log: LogHandler = (type, detail) => this.tradeLog.push(type, detail);

    safeSubscribe<AsterAccountSnapshot>(
      this.exchange.watchAccount.bind(this.exchange),
      (snapshot) => {
        this.accountSnapshot = snapshot;
        const totalUnrealized = Number(snapshot.totalUnrealizedProfit ?? "0");
        if (Number.isFinite(totalUnrealized)) {
          this.accountUnrealized = totalUnrealized;
        }
        const position = getPosition(snapshot, this.config.symbol);
        this.sessionVolume.update(position, this.getReferencePrice());
        this.feedStatus.account = true;
        this.emitUpdate();
      },
      log,
      {
        subscribeFail: (error) => t("log.subscribe.accountFail", { error: String(error) }),
        processFail: (error) => t("log.process.accountError", { error: String(error) }),
      }
    );

    safeSubscribe<AsterOrder[]>(
      this.exchange.watchOrders.bind(this.exchange),
      (orders) => {
        this.syncLocksWithOrders(orders);
        this.openOrders = Array.isArray(orders)
          ? orders.filter(
              (order) =>
                order.type !== "MARKET" &&
                order.symbol === this.config.symbol &&
                isOrderActiveStatus(order.status)
            )
          : [];
        const currentIds = new Set(this.openOrders.map((order) => String(order.orderId)));
        for (const id of Array.from(this.pendingCancelOrders)) {
          if (!currentIds.has(id)) {
            this.pendingCancelOrders.delete(id);
          }
        }
        this.initialOrderSnapshotReady = true;
        this.feedStatus.orders = true;
        this.emitUpdate();
      },
      log,
      {
        subscribeFail: (error) => t("log.subscribe.orderFail", { error: String(error) }),
        processFail: (error) => t("log.process.orderError", { error: String(error) }),
      }
    );

    safeSubscribe<AsterDepth>(
      this.exchange.watchDepth.bind(this.exchange, this.config.symbol),
      (depth) => {
        this.depthSnapshot = depth;
        this.feedStatus.depth = true;
        this.emitUpdate();
      },
      log,
      {
        subscribeFail: (error) => t("log.subscribe.depthFail", { error: String(error) }),
        processFail: (error) => t("log.process.depthError", { error: String(error) }),
      }
    );

    safeSubscribe<AsterTicker>(
      this.exchange.watchTicker.bind(this.exchange, this.config.symbol),
      (ticker) => {
        this.tickerSnapshot = ticker;
        this.feedStatus.ticker = true;
        this.emitUpdate();
      },
      log,
      {
        subscribeFail: (error) => t("log.subscribe.tickerFail", { error: String(error) }),
        processFail: (error) => t("log.process.tickerError", { error: String(error) }),
      }
    );
  }

  private syncLocksWithOrders(orders: AsterOrder[] | null | undefined): void {
    const list = Array.isArray(orders) ? orders : [];
    Object.keys(this.pending).forEach((type) => {
      const pendingId = this.pending[type];
      if (!pendingId) return;
      const match = list.find((order) => String(order.orderId) === pendingId);
      if (!match || (match.status && match.status !== "NEW" && match.status !== "PARTIALLY_FILLED")) {
        unlockOperating(this.locks, this.timers, this.pending, type);
      }
    });
  }

  private isReady(): boolean {
    return Boolean(
      this.feedStatus.account &&
        this.feedStatus.depth &&
        this.feedStatus.ticker &&
        this.feedStatus.orders
    );
  }

  private async tick(): Promise<void> {
    if (this.processing) return;
    this.processing = true;
    let hadRateLimit = false;
    try {
      const decision = this.rateLimit.beforeCycle();
      if (decision === "paused") {
        this.emitUpdate();
        return;
      }
      if (decision === "skip") {
        return;
      }
      if (!this.isReady()) {
        this.logReadinessBlockers();
        this.emitUpdate();
        return;
      }
      this.resetReadinessFlags();
      if (!(await this.ensureStartupOrderReset())) {
        this.emitUpdate();
        return;
      }

      const depth = this.depthSnapshot!;
      const { topBid, topAsk } = getTopPrices(depth);
      if (topBid == null || topAsk == null) {
        this.emitUpdate();
        return;
      }

      const position = getPosition(this.accountSnapshot, this.config.symbol);
      const absPosition = Math.abs(position.positionAmt);
      const closeThreshold = Number(this.config.closeThreshold);
      const closeOnly =
        Number.isFinite(closeThreshold) &&
        closeThreshold > 0 &&
        absPosition >= closeThreshold - EPS;
      const prevCloseOnly = this.lastCloseOnly;
      if (closeOnly !== prevCloseOnly) {
        this.tradeLog.push("info", closeOnly ? "进入平仓模式，仅挂 reduce-only" : "退出平仓模式");
        this.lastCloseOnly = closeOnly;
      }


      const binanceSnapshot = this.binanceDepth.getSnapshot();
      const rawSkipBuy = Boolean(binanceSnapshot?.skipBuySide);
      const rawSkipSell = Boolean(binanceSnapshot?.skipSellSide);
      const skipBuy = closeOnly ? false : rawSkipBuy;
      const skipSell = closeOnly ? false : rawSkipSell;
      const prevSkipBuy = this.lastSkipBuy;
      const prevSkipSell = this.lastSkipSell;
      if (skipBuy !== prevSkipBuy || skipSell !== prevSkipSell) {
        if (skipBuy || skipSell) {
          const summary = `${skipBuy ? "BUY" : ""}${skipBuy && skipSell ? "/" : ""}${skipSell ? "SELL" : ""}`;
          this.tradeLog.push("info", `Binance 深度失衡，暂停 ${summary} 挂单`);
        } else {
          this.tradeLog.push("info", "Binance 深度恢复，继续挂单");
        }
        this.lastSkipBuy = skipBuy;
        this.lastSkipSell = skipSell;
      }

      const closeOnlyChanged = closeOnly !== prevCloseOnly;
      const skipChanged = skipBuy !== prevSkipBuy || skipSell !== prevSkipSell;
      const repriceNeeded = closeOnly ? true : this.shouldReprice(topBid, topAsk);
      const shouldRecompute =
        closeOnly ||
        repriceNeeded ||
        closeOnlyChanged ||
        skipChanged ||
        this.desiredOrders.length === 0;

      const desired = shouldRecompute
        ? closeOnly
          ? this.buildCloseOnlyOrders(position, topBid, topAsk)
          : this.buildDesiredOrders({
              bid1: topBid,
              ask1: topAsk,
              skipBuy,
              skipSell,
            })
        : this.desiredOrders;

      if (shouldRecompute) {
        if (closeOnly) {
          this.lastQuoteBid1 = null;
          this.lastQuoteAsk1 = null;
        } else {
          this.lastQuoteBid1 = topBid;
          this.lastQuoteAsk1 = topAsk;
        }
      }

      this.desiredOrders = desired;
      this.logDesiredOrders(desired);
      this.sessionVolume.update(position, this.getReferencePrice());
      await this.syncOrders(desired, closeOnly);
      this.emitUpdate();
    } catch (error) {
      if (isRateLimitError(error)) {
        hadRateLimit = true;
        this.rateLimit.registerRateLimit("maker-points");
        this.tradeLog.push("warn", `限频触发，暂停挂单: ${extractMessage(error)}`);
      } else {
        this.tradeLog.push("error", `MakerPoints 主循环异常: ${extractMessage(error)}`);
      }
      this.emitUpdate();
    } finally {
      this.rateLimit.onCycleComplete(hadRateLimit);
      this.processing = false;
    }
  }

  private buildDesiredOrders(params: {
    bid1: number;
    ask1: number;
    skipBuy: boolean;
    skipSell: boolean;
  }): DesiredOrder[] {
    const { bid1, ask1, skipBuy, skipSell } = params;
    const amount = Number(this.config.perOrderAmount);
    if (!Number.isFinite(amount) || amount <= 0) return [];

    const targets = buildBpsTargets({
      band0To10: this.config.enableBand0To10,
      band10To30: this.config.enableBand10To30,
      band30To100: this.config.enableBand30To100,
    }).sort((a, b) => b - a);

    if (!targets.length) return [];

    const priceDecimals = this.getPriceDecimals();
    const desired: DesiredOrder[] = [];

    for (const bps of targets) {
      if (!skipBuy) {
        const price = bid1 * (1 - bps / 10000);
        if (Number.isFinite(price) && price > 0) {
          desired.push({
            side: "BUY",
            price: formatPriceToString(price, priceDecimals),
            amount,
            reduceOnly: false,
          });
        }
      }
      if (!skipSell) {
        const price = ask1 * (1 + bps / 10000);
        if (Number.isFinite(price) && price > 0) {
          desired.push({
            side: "SELL",
            price: formatPriceToString(price, priceDecimals),
            amount,
            reduceOnly: false,
          });
        }
      }
    }

    return desired;
  }

  private buildCloseOnlyOrders(
    position: PositionSnapshot,
    bid1: number,
    ask1: number
  ): DesiredOrder[] {
    const absPosition = Math.abs(position.positionAmt);
    if (absPosition < EPS) return [];
    const priceDecimals = this.getPriceDecimals();
    if (position.positionAmt > 0) {
      return [
        {
          side: "SELL",
          price: formatPriceToString(bid1, priceDecimals),
          amount: absPosition,
          reduceOnly: true,
        },
      ];
    }
    return [
      {
        side: "BUY",
        price: formatPriceToString(ask1, priceDecimals),
        amount: absPosition,
        reduceOnly: true,
      },
    ];
  }

  private shouldReprice(bid1: number, ask1: number): boolean {
    const threshold = Number(this.config.minRepriceBps);
    if (!Number.isFinite(threshold) || threshold <= 0) return true;
    if (!Number.isFinite(bid1) || !Number.isFinite(ask1)) return false;
    if (!Number.isFinite(this.lastQuoteBid1 ?? NaN) || !Number.isFinite(this.lastQuoteAsk1 ?? NaN)) {
      return true;
    }
    if ((this.lastQuoteBid1 ?? 0) <= 0 || (this.lastQuoteAsk1 ?? 0) <= 0) return true;
    const bidMove = Math.abs(bid1 - (this.lastQuoteBid1 ?? bid1)) / (this.lastQuoteBid1 ?? bid1) * 10000;
    const askMove = Math.abs(ask1 - (this.lastQuoteAsk1 ?? ask1)) / (this.lastQuoteAsk1 ?? ask1) * 10000;
    return bidMove >= threshold || askMove >= threshold;
  }

  private async ensureStartupOrderReset(): Promise<boolean> {
    if (this.initialOrderResetDone) return true;
    if (!this.initialOrderSnapshotReady) return false;
    if (!this.openOrders.length) {
      this.initialOrderResetDone = true;
      return true;
    }
    try {
      await this.exchange.cancelAllOrders({ symbol: this.config.symbol });
      this.pendingCancelOrders.clear();
      unlockOperating(this.locks, this.timers, this.pending, "LIMIT");
      this.openOrders = [];
      this.emitUpdate();
      this.tradeLog.push("order", "启动时清理历史挂单");
      this.initialOrderResetDone = true;
      return true;
    } catch (error) {
      if (isUnknownOrderError(error)) {
        this.tradeLog.push("order", "历史挂单已消失，跳过启动清理");
        this.initialOrderResetDone = true;
        this.openOrders = [];
        this.emitUpdate();
        return true;
      }
      this.tradeLog.push("error", `启动撤单失败: ${String(error)}`);
      return false;
    }
  }

  private async syncOrders(targets: DesiredOrder[], closeOnly: boolean): Promise<void> {
    const availableOrders = this.openOrders.filter((o) => !this.pendingCancelOrders.has(String(o.orderId)));
    const openOrders = availableOrders.filter((order) => isOrderActiveStatus(order.status));
    const { toCancel, toPlace } = makeOrderPlan(openOrders, targets);

    for (const order of toCancel) {
      if (this.pendingCancelOrders.has(String(order.orderId))) continue;
      this.pendingCancelOrders.add(String(order.orderId));
      await safeCancelOrder(
        this.exchange,
        this.config.symbol,
        order,
        () => {
          this.tradeLog.push(
            "order",
            `撤销不匹配订单 ${order.side} @ ${order.price} reduceOnly=${order.reduceOnly}`
          );
        },
        () => {
          this.tradeLog.push("order", "撤销时发现订单已被成交/取消，忽略");
          this.pendingCancelOrders.delete(String(order.orderId));
          this.openOrders = this.openOrders.filter((existing) => existing.orderId !== order.orderId);
        },
        (error) => {
          this.tradeLog.push("error", `撤销订单失败: ${String(error)}`);
          this.pendingCancelOrders.delete(String(order.orderId));
          this.openOrders = this.openOrders.filter((existing) => existing.orderId !== order.orderId);
        }
      );
    }

    const insufficientActive = this.applyInsufficientBalanceState(Date.now());
    if (this.rateLimit.shouldBlockEntries() || insufficientActive) {
      return;
    }

    for (const target of toPlace) {
      if (!target) continue;
      if (target.amount < EPS) continue;
      try {
        await placeOrder(
          this.exchange,
          this.config.symbol,
          this.openOrders,
          this.locks,
          this.timers,
          this.pending,
          target.side,
          target.price,
          target.amount,
          (type, detail) => this.tradeLog.push(type, detail),
          target.reduceOnly,
          undefined,
          {
            priceTick: this.priceTick,
            qtyStep: this.qtyStep,
            skipDedupe: true,
          }
        );
      } catch (error) {
        if (isInsufficientBalanceError(error)) {
          this.registerInsufficientBalance(error);
          break;
        }
        this.tradeLog.push(
          "error",
          `挂单失败 ${target.side} @ ${target.price}: ${extractMessage(error)}`
        );
      }
    }
  }

  private async checkStopLoss(): Promise<void> {
    if (this.stopLossProcessing) return;
    const lossLimit = Number(this.config.stopLossUsd);
    if (!Number.isFinite(lossLimit) || lossLimit <= 0) return;
    if (!this.accountSnapshot) return;
    const position = getPosition(this.accountSnapshot, this.config.symbol);
    const absPosition = Math.abs(position.positionAmt);
    if (absPosition < EPS) return;
    if (!Number.isFinite(position.unrealizedProfit)) return;

    const now = Date.now();
    if (now < this.stopLossCooldownUntil) return;
    if (position.unrealizedProfit > -lossLimit) return;

    this.stopLossProcessing = true;
    this.stopLossCooldownUntil = now + STOP_LOSS_COOLDOWN_MS;
    this.tradeLog.push(
      "stop",
      `触发止损: 未实现亏损 ${position.unrealizedProfit.toFixed(4)} USDT`
    );
    try {
      await this.flushOrders();
      await marketClose(
        this.exchange,
        this.config.symbol,
        this.openOrders,
        this.locks,
        this.timers,
        this.pending,
        position.positionAmt > 0 ? "SELL" : "BUY",
        absPosition,
        (type, detail) => this.tradeLog.push(type, detail),
        undefined,
        { qtyStep: this.qtyStep }
      );
    } catch (error) {
      if (isUnknownOrderError(error)) {
        this.tradeLog.push("order", "止损平仓时订单已不存在");
      } else {
        this.tradeLog.push("error", `止损平仓失败: ${extractMessage(error)}`);
      }
    } finally {
      this.stopLossProcessing = false;
      this.emitUpdate();
    }
  }

  private async flushOrders(): Promise<void> {
    if (!this.openOrders.length) return;
    for (const order of this.openOrders) {
      if (this.pendingCancelOrders.has(String(order.orderId))) continue;
      this.pendingCancelOrders.add(String(order.orderId));
      await safeCancelOrder(
        this.exchange,
        this.config.symbol,
        order,
        () => {
          // No log on successful cancel
        },
        () => {
          this.tradeLog.push("order", "撤销时发现订单已被成交/取消，忽略");
          this.pendingCancelOrders.delete(String(order.orderId));
          this.openOrders = this.openOrders.filter((existing) => existing.orderId !== order.orderId);
        },
        (error) => {
          this.tradeLog.push("error", `撤销订单失败: ${String(error)}`);
          this.pendingCancelOrders.delete(String(order.orderId));
          this.openOrders = this.openOrders.filter((existing) => existing.orderId !== order.orderId);
        }
      );
    }
  }

  private syncPrecision(): void {
    if (this.precisionSync) return;
    const getPrecision = this.exchange.getPrecision?.bind(this.exchange);
    if (!getPrecision) return;
    this.precisionSync = getPrecision()
      .then((precision) => {
        if (!precision) return;
        let updated = false;
        if (Number.isFinite(precision.priceTick) && precision.priceTick > 0) {
          if (Math.abs(precision.priceTick - this.priceTick) > 1e-12) {
            this.priceTick = precision.priceTick;
            this.config.priceTick = precision.priceTick;
            updated = true;
          }
        }
        if (Number.isFinite(precision.qtyStep) && precision.qtyStep > 0) {
          if (Math.abs(precision.qtyStep - this.qtyStep) > 1e-12) {
            this.qtyStep = precision.qtyStep;
            updated = true;
          }
        }
        if (updated) {
          this.tradeLog.push(
            "info",
            t("log.common.precisionSynced", {
              priceTick: precision.priceTick,
              qtyStep: precision.qtyStep,
            })
          );
        }
      })
      .catch((error) => {
        this.tradeLog.push("error", t("log.common.precisionFailed", { error: extractMessage(error) }));
        this.precisionSync = null;
        setTimeout(() => this.syncPrecision(), 2000);
      });
  }

  private getPriceDecimals(): number {
    const tick = Math.max(1e-9, this.priceTick);
    const raw = Math.log10(1 / tick);
    if (!Number.isFinite(raw)) return 0;
    return Math.max(0, Math.floor(raw + 1e-9));
  }

  private emitUpdate(): void {
    try {
      const snapshot = this.buildSnapshot();
      this.events.emit("update", snapshot, (error) => {
        this.tradeLog.push("error", `更新监听异常: ${String(error)}`);
      });
    } catch (err) {
      this.tradeLog.push("error", `快照生成异常: ${String(err)}`);
    }
  }

  private buildSnapshot(): MakerPointsSnapshot {
    const position = getPosition(this.accountSnapshot, this.config.symbol);
    const { topBid, topAsk } = getTopPrices(this.depthSnapshot);
    const spread = topBid != null && topAsk != null ? topAsk - topBid : null;
    const pnl = computePositionPnl(position, topBid, topAsk);

    return {
      ready: this.isReady(),
      symbol: this.config.symbol,
      topBid,
      topAsk,
      spread,
      priceDecimals: this.getPriceDecimals(),
      position,
      pnl,
      accountUnrealized: this.accountUnrealized,
      sessionVolume: this.sessionVolume.value,
      openOrders: this.openOrders,
      desiredOrders: this.desiredOrders,
      tradeLog: this.tradeLog.all(),
      lastUpdated: Date.now(),
      feedStatus: { ...this.feedStatus },
      binanceDepth: this.binanceDepth.getSnapshot(),
      quoteStatus: {
        closeOnly: this.lastCloseOnly,
        skipBuy: this.lastSkipBuy,
        skipSell: this.lastSkipSell,
      },
    };
  }

  private getReferencePrice(): number | null {
    return getMidOrLast(this.depthSnapshot, this.tickerSnapshot);
  }

  private logReadinessBlockers(): void {
    if (!this.feedStatus.account && !this.readinessLogged.account) {
      this.tradeLog.push("info", t("log.maker.waitAccount"));
      this.readinessLogged.account = true;
    }
    if (!this.feedStatus.depth && !this.readinessLogged.depth) {
      this.tradeLog.push("info", t("log.maker.waitDepth"));
      this.readinessLogged.depth = true;
    }
    if (!this.feedStatus.ticker && !this.readinessLogged.ticker) {
      this.tradeLog.push("info", t("log.maker.waitTicker"));
      this.readinessLogged.ticker = true;
    }
    if (!this.feedStatus.orders && !this.readinessLogged.orders) {
      this.tradeLog.push("info", t("log.maker.waitOrders"));
      this.readinessLogged.orders = true;
    }
  }

  private resetReadinessFlags(): void {
    this.readinessLogged = {
      account: false,
      depth: false,
      ticker: false,
      orders: false,
    };
  }

  private logDesiredOrders(desired: DesiredOrder[]): void {
    if (!desired.length) {
      if (this.lastDesiredSummary !== "none") {
        this.tradeLog.push("info", "暂无目标挂单");
        this.lastDesiredSummary = "none";
      }
      return;
    }
    const summary = desired
      .map((order) => `${order.side}@${order.price}${order.reduceOnly ? "(RO)" : ""}`)
      .join(" | ");
    if (summary !== this.lastDesiredSummary) {
      this.tradeLog.push("info", `目标挂单: ${summary}`);
      this.lastDesiredSummary = summary;
    }
  }

  private registerInsufficientBalance(error: unknown): void {
    const now = Date.now();
    const detail = extractMessage(error);
    const alreadyActive = now < this.insufficientBalanceCooldownUntil;
    if (alreadyActive && detail === this.lastInsufficientMessage) {
      this.insufficientBalanceCooldownUntil = now + INSUFFICIENT_BALANCE_COOLDOWN_MS;
      return;
    }
    this.insufficientBalanceCooldownUntil = now + INSUFFICIENT_BALANCE_COOLDOWN_MS;
    this.lastInsufficientMessage = detail;
    const seconds = Math.ceil(INSUFFICIENT_BALANCE_COOLDOWN_MS / 1000);
    this.tradeLog.push("warn", `余额不足，暂停挂单 ${seconds}s: ${detail}`);
    this.insufficientBalanceNotified = true;
  }

  private applyInsufficientBalanceState(now: number): boolean {
    const active = now < this.insufficientBalanceCooldownUntil;
    if (!active && this.insufficientBalanceNotified) {
      this.tradeLog.push("info", "余额恢复，继续挂单");
      this.insufficientBalanceNotified = false;
      this.lastInsufficientMessage = null;
    }
    return active;
  }
}

function resolveBinanceSymbol(symbol: string): string {
  const parts = parseSymbolParts(symbol);
  const base = (parts.base ?? symbol).replace(/[^a-zA-Z0-9]/g, "").toUpperCase();
  return base ? `${base}USDT` : "BTCUSDT";
}
