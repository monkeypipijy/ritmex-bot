# StandX 做市积分策略使用教程（超详细新手版）

本教程会 **手把手** 教你如何运行 StandX 做市积分策略。每一步都有详细说明，按顺序操作即可。

---

## 第一步：安装 Bun（运行环境）

本项目需要 Bun 才能运行。

### macOS / Linux 用户：
打开终端，复制粘贴以下命令后按回车：
```bash
curl -fsSL https://bun.sh/install | bash
```

### Windows 用户：
打开 PowerShell，复制粘贴以下命令后按回车：
```powershell
powershell -c "irm bun.sh/install.ps1 | iex"
```

安装完成后，**关闭终端，重新打开一个新的终端窗口**，然后输入：
```bash
bun -v
```
如果显示版本号（如 `1.2.x`），说明安装成功。

---

## 第二步：下载项目并安装依赖

```bash
git clone https://github.com/discountry/ritmex-bot.git
cd ritmex-bot
bun install
```

---

## 第三步：获取 StandX 登录凭证（最重要的一步）

> ⚠️ **这一步是 90% 新手卡住的地方，请仔细阅读！**
>
> ⚠️ **这一步是 90% 新手卡住的地方，请仔细阅读！**
>
> ⚠️ **这一步是 90% 新手卡住的地方，请仔细阅读！**

策略需要两样东西才能帮你下单：
1. **TOKEN**（登录令牌）
2. **代理钱包私钥**（用于签名交易）

### 获取步骤（图文说明）：

#### 3.1 打开专用登录页面

在浏览器打开这个网址：
```
https://standx.ritmex.one/
```

> **注意：不是 standx.com！是 standx.ritmex.one！**
>
> **注意：不是 standx.com！是 standx.ritmex.one！**
>
> **注意：不是 standx.com！是 standx.ritmex.one！**

#### 3.2 连接你的钱包

点击页面上的 **"连接钱包"** 按钮，使用 MetaMask 或其他钱包连接。

#### 3.3 点击登录

连接钱包后，点击 **"登录"** 按钮。钱包会弹出签名请求，确认签名。

#### 3.4 导出登录信息（关键！）

登录成功后，页面上会出现 **"导出登录信息"** 按钮，**点击它**！

你会看到类似这样的内容：

```
Token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.xxxxx...（很长一串）

代理钱包私钥: 1234567890abcdef...（64 位十六进制字符，一般不带 0x 前缀）
```

> 🔴 **请把这两个值复制保存下来！**
>
> 🔴 **请把这两个值复制保存下来！**
>
> 🔴 **请把这两个值复制保存下来！**

### 什么是代理钱包？

- 代理钱包是系统 **自动为你生成** 的一个临时钱包
- 它 **只用于签名交易**，不存放你的资金
- 你的资产仍然在你自己的钱包里，非常安全
- **你不需要手动创建**，登录时系统会自动生成

---

## 第四步：配置环境变量

在项目根目录创建一个 `.env` 文件（如果已存在就修改它）。

### 4.1 创建/编辑 .env 文件

**macOS / Linux：**
```bash
nano .env
```

**Windows：**
用记事本打开项目文件夹，新建一个文本文件，命名为 `.env`（注意前面有个点）

### 4.2 填入以下内容

> ⚠️ **请务必把下面的示例值替换成你自己的！**
>
> ⚠️ **请务必把下面的示例值替换成你自己的！**
>
> ⚠️ **请务必把下面的示例值替换成你自己的！**

```bash
# ===== 交易所设置 =====
EXCHANGE=standx

# ===== 你的登录凭证（第三步获取的） =====
# 把下面的 "你的TOKEN" 替换成你导出的 Token（很长一串以 eyJ 开头的）
STANDX_TOKEN=你的TOKEN

# 把下面的 "你的私钥" 替换成页面中标注为 STANDX_REQUEST_PRIVATE_KEY 的那一段内容（站点导出的值本身一般不带 0x 前缀，按页面原样粘贴即可）
STANDX_REQUEST_PRIVATE_KEY=你的 STANDX_REQUEST_PRIVATE_KEY

# ===== 交易品种 =====
STANDX_SYMBOL=BTC-USD

# ===== 策略参数（新手直接用默认值就行） =====
MAKER_POINTS_ORDER_AMOUNT=0.01
MAKER_POINTS_CLOSE_THRESHOLD=0.1
MAKER_POINTS_STOP_LOSS_USD=0
MAKER_POINTS_MIN_REPRICE_BPS=3

# ===== 挂单档位开关 =====
MAKER_POINTS_BAND_0_10=true
MAKER_POINTS_BAND_10_30=true
MAKER_POINTS_BAND_30_100=true

# ===== Token 过期时间配置（可选） =====
# 配置 Token 过期时间后，策略会在 Token 过期前提醒你，过期后自动进入安全模式
# 格式1：时间戳（毫秒），例如：1735689600000
# 格式2：时间戳（秒），例如：1735689600（会自动转换为毫秒）
# 格式3：ISO 日期字符串，例如：2025-01-01T00:00:00Z
# 如果不配置此项，策略不会检查 Token 过期时间
# STANDX_TOKEN_EXPIRY=1735689600000

# ===== Telegram 通知配置（可选） =====
# 配置后，策略会通过 Telegram 发送重要通知（订单成交、开仓、平仓、止损、Token过期等）
# 如何获取 Bot Token：在 Telegram 搜索 @BotFather，发送 /newbot 创建机器人，获取 Token
# 如何获取 Chat ID：在 Telegram 搜索 @userinfobot，发送任意消息即可看到你的 Chat ID
# TELEGRAM_BOT_TOKEN=你的BotToken
# TELEGRAM_CHAT_ID=你的ChatID
# TELEGRAM_ACCOUNT_LABEL=我的账户（可选，用于区分多个账户的通知）
```

### 正确填写示例

假设你导出的信息是：
- Token: `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ`
- 代理钱包私钥: `abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890`（页面导出的值本身不带 `0x`）

那么你的 `.env` 应该这样写：

```bash
EXCHANGE=standx
STANDX_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ
STANDX_REQUEST_PRIVATE_KEY=abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890
STANDX_SYMBOL=BTC-USD
MAKER_POINTS_ORDER_AMOUNT=0.01
MAKER_POINTS_CLOSE_THRESHOLD=0.1
MAKER_POINTS_STOP_LOSS_USD=0
MAKER_POINTS_MIN_REPRICE_BPS=3
MAKER_POINTS_BAND_0_10=true
MAKER_POINTS_BAND_10_30=true
MAKER_POINTS_BAND_30_100=true
# STANDX_TOKEN_EXPIRY=1735689600000
# TELEGRAM_BOT_TOKEN=你的BotToken
# TELEGRAM_CHAT_ID=你的ChatID
```

> 🔴 **不要加引号！不要加空格！直接粘贴值！**
>
> 🔴 **不要加引号！不要加空格！直接粘贴值！**
>
> 🔴 **不要加引号！不要加空格！直接粘贴值！**

---

## 第五步：启动策略

### 普通启动（看实时仪表盘）
```bash
bun run index.ts --strategy maker-points --exchange standx
```

### 后台运行（推荐长期挂机）
```bash
bun run pm2:start:maker-points
```

---

## 配置参数说明

| 参数 | 含义 | 新手建议 |
|------|------|----------|
| `STANDX_TOKEN` | 登录令牌 | 必填，从第三步获取 |
| `STANDX_REQUEST_PRIVATE_KEY` | 代理钱包私钥 | 必填，从第三步获取 |
| `STANDX_SYMBOL` | 交易品种 | 默认 `BTC-USD` |
| `MAKER_POINTS_ORDER_AMOUNT` | 每笔挂单数量 | 建议 `0.01` 起步 |
| `MAKER_POINTS_CLOSE_THRESHOLD` | 持仓达到多少开始平仓 | 设为 `0` 表示不自动平仓 |
| `MAKER_POINTS_STOP_LOSS_USD` | 亏损多少美元强制平仓 | 设为 `0` 表示关闭止损 |
| `MAKER_POINTS_BAND_*` | 三个挂单档位的开关 | 全部 `true` 即可 |
| `STANDX_TOKEN_EXPIRY` | Token 过期时间 | 可选，格式见下方说明 |
| `TELEGRAM_BOT_TOKEN` | Telegram 机器人 Token | 可选，用于接收通知 |
| `TELEGRAM_CHAT_ID` | Telegram 聊天 ID | 可选，配合 Bot Token 使用 |
| `TELEGRAM_ACCOUNT_LABEL` | Telegram 通知账户标签 | 可选，用于区分多个账户 |

### Token 过期时间配置详解

`STANDX_TOKEN_EXPIRY` 用于设置 Token 的过期时间。配置后，策略会：

1. **Token 过期前 1 小时**：在日志中提醒你 Token 即将过期
2. **Token 过期后**：
   - 如果有持仓：进入**平仓模式**，只允许平仓和止损，不再开新仓
   - 如果无持仓但有挂单：**自动取消所有挂单**
   - 如果无持仓无挂单：进入**静默模式**，只接收数据，不下单

**支持的格式：**
- **时间戳（毫秒）**：`1735689600000`
- **时间戳（秒）**：`1735689600`（会自动转换为毫秒）
- **ISO 日期字符串**：`2025-01-01T00:00:00Z` 或 `2025-01-01 00:00:00`

**如何获取 Token 过期时间？**

登录 standx.ritmex.one 时，系统会返回 Token 的有效期。你可以在导出登录信息时查看，或者根据登录时设置的过期时间计算。

**示例：**
```bash
# 方式1：使用时间戳（毫秒）
STANDX_TOKEN_EXPIRY=1735689600000

# 方式2：使用时间戳（秒）
STANDX_TOKEN_EXPIRY=1735689600

# 方式3：使用 ISO 日期字符串
STANDX_TOKEN_EXPIRY=2025-01-01T00:00:00Z
```

> 💡 **提示**：如果不配置此项，策略不会检查 Token 过期时间，但建议配置以便及时收到提醒。

### Telegram 通知配置详解

配置 Telegram 通知后，策略会在以下情况发送通知：

- 📝 **订单成交**：挂单被成交时
- 📈 **开仓**：持仓从 0 变为非 0 时
- 📉 **平仓**：持仓从非 0 变为 0 时
- 🛑 **止损触发**：触发止损平仓时
- ⏰ **Token 过期**：Token 过期时

**配置步骤：**

1. **创建 Telegram 机器人**：
   - 在 Telegram 搜索 `@BotFather`
   - 发送 `/newbot` 命令
   - 按提示设置机器人名称和用户名
   - 获取 Bot Token（格式类似：`123456789:ABCdefGHIjklMNOpqrsTUVwxyz`）

2. **获取你的 Chat ID**：
   - 在 Telegram 搜索 `@userinfobot`
   - 发送任意消息
   - 机器人会返回你的 Chat ID（一串数字，例如：`123456789`）

3. **配置环境变量**：
   ```bash
   TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
   TELEGRAM_CHAT_ID=123456789
   TELEGRAM_ACCOUNT_LABEL=我的账户（可选）
   ```

4. **测试通知**：
   - 启动策略后，如果配置正确，会在 Token 过期或重要事件时收到通知
   - 如果收不到通知，检查 Bot Token 和 Chat ID 是否正确

> 💡 **提示**：`TELEGRAM_ACCOUNT_LABEL` 是可选的，如果你有多个账户在运行策略，可以用这个标签区分不同账户的通知。

---

## 常见问题

### Q：报错说 Token 无效怎么办？

重新去 https://standx.ritmex.one/ 登录，重新导出 Token。Token 可能过期了。

### Q：代理钱包私钥从哪来的？

登录 standx.ritmex.one 后点击"导出登录信息"就能看到。
**你不需要自己创建钱包，系统会自动生成！**
**你不需要自己创建钱包，系统会自动生成！**
**你不需要自己创建钱包，系统会自动生成！**

### Q：.env 文件放在哪？

放在项目根目录，就是 `ritmex-bot` 文件夹里，和 `package.json` 同一个目录。

### Q：为什么策略没有下单？

1. 检查账户里有没有足够的保证金
2. 检查 TOKEN 和私钥是否正确填写
3. 检查 .env 文件是否保存成功

### Q：担心平掉我手动开的仓位？

把 `MAKER_POINTS_CLOSE_THRESHOLD` 设为 `0` 或者设置成一个比你持仓大的数字。

### Q：如何知道 Token 什么时候过期？

配置 `STANDX_TOKEN_EXPIRY` 环境变量，策略会在 Token 过期前 1 小时提醒你。Token 过期后，如果有持仓会进入平仓模式，只允许平仓和止损。

### Q：Telegram 通知收不到怎么办？

1. 检查 `TELEGRAM_BOT_TOKEN` 和 `TELEGRAM_CHAT_ID` 是否正确填写
2. 确保没有在 Bot Token 和 Chat ID 前后加引号或空格
3. 在 Telegram 中先给机器人发送一条消息（任意内容），然后再启动策略
4. 检查网络连接是否正常

---

## 安全提示

1. **绝对不要把 TOKEN 和私钥分享给任何人！**
2. **绝对不要把 TOKEN 和私钥分享给任何人！**
3. **绝对不要把 TOKEN 和私钥分享给任何人！**

代理钱包只用于签名，你的资产始终在你自己的主钱包里。但如果泄露了 TOKEN，别人可以用你的账户交易。

---

## 还是不会？

把你的报错信息截图发到 Telegram 群里，会有人帮你：

Telegram 群：https://t.me/+4fdo0quY87o4Mjhh
