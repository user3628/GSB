# 数据集声明

固定生成种子：`20261120`。生成脚本：`scripts/generate_samples.py`。
所有 CSV 使用 UTF-8、LF 换行和固定列顺序；缺失值以空字段表示。

## small

- 文件：`small.csv`
- SHA-256：`620f1f1d3757e28a3cfe114f30cfdb0f72aa2f20a92f8649a935d812c4964b02`
- 行数：600
- 列数：12
- 目标列：`label`
- 类别计数：`{"0": 304, "1": 296}`
- 缺失列：无
- 冗余列：无

| 列 | 角色 | 语义 |
| --- | --- | --- |
| `age` | continuous | Client age at application. |
| `income` | continuous | Annual gross income. |
| `tenure_months` | continuous | Months since the relationship began. |
| `monthly_usage` | continuous | Product sessions in an average month. |
| `debt_ratio` | continuous | Debt service divided by income. |
| `credit_score` | continuous | Bureau credit score. |
| `product_count` | continuous | Active products held by the client. |
| `region` | categorical | Sales region. |
| `plan` | categorical | Subscription plan. |
| `channel` | categorical | Acquisition channel. |
| `segment` | categorical | Commercial segment. |

- 600 rows and 12 columns including the target.
- Seven continuous, four categorical and one binary target column.
- No missing values; intended for fast interactive experiments.

## medium

- 文件：`medium.csv`
- SHA-256：`cdb30d262ec719ced4039b37a440f2d69354a1e04daf239a8a3844d12c074fa1`
- 行数：6000
- 列数：40
- 目标列：`label`
- 类别计数：`{"0": 2491, "1": 3509}`
- 缺失列：`income`, `credit_score`, `satisfaction`, `payment_method`, `industry`
- 冗余列：`income_copy`, `usage_per_day`, `sessions_x_minutes`, `risk_average`, `tenure_years`, `noisy_tenure`, `noisy_credit`, `satisfaction_copy`, `engagement_index`

| 列 | 角色 | 语义 |
| --- | --- | --- |
| `age` | continuous | Client age at application. |
| `income` | continuous | Annual gross income. |
| `tenure_months` | continuous | Months since relationship start. |
| `monthly_usage` | continuous | Product sessions per month. |
| `avg_session_minutes` | continuous | Average session duration. |
| `weekly_sessions` | continuous | Sessions in an average week. |
| `support_tickets` | continuous | Tickets opened in 90 days. |
| `late_payments` | continuous | Late payments in 12 months. |
| `debt_ratio` | continuous | Debt service divided by income. |
| `credit_score` | continuous | Bureau credit score. |
| `contract_value` | continuous | Annualized contract value. |
| `discount_rate` | continuous | Applied discount rate. |
| `marketing_touches` | continuous | Marketing touches in 30 days. |
| `email_open_rate` | continuous | Rolling email open rate. |
| `mobile_share` | continuous | Share of mobile sessions. |
| `satisfaction` | continuous | Latest satisfaction score. |
| `churn_risk_score` | continuous | Calibrated churn risk score. |
| `onboarding_days` | continuous | Days from signup to activation. |
| `product_count` | continuous | Active products. |
| `payment_delay_days` | continuous | Average payment delay. |
| `income_copy` | continuous | Redundant copy of income. |
| `usage_per_day` | continuous | Daily usage derived from monthly usage. |
| `sessions_x_minutes` | continuous | Session intensity interaction. |
| `risk_average` | continuous | Composite risk indicator. |
| `tenure_years` | continuous | Tenure expressed in years. |
| `noisy_tenure` | continuous | Noisy tenure duplicate. |
| `noisy_credit` | continuous | Noisy credit duplicate. |
| `satisfaction_copy` | continuous | Redundant copy of satisfaction. |
| `engagement_index` | continuous | Composite engagement index. |
| `region` | categorical | Sales region. |
| `plan` | categorical | Subscription plan. |
| `channel` | categorical | Acquisition channel. |
| `segment` | categorical | Commercial segment. |
| `device` | categorical | Primary device. |
| `contract_type` | categorical | Contract duration. |
| `payment_method` | categorical | Payment method. |
| `industry` | categorical | Industry vertical. |
| `service_tier` | categorical | Service tier. |
| `acquisition_source` | categorical | Acquisition source. |

- 6000 rows and 40 columns including the target.
- Twenty-nine continuous, ten categorical and one binary target column.
- Contains exact copies, linear derivatives, composites and two noisy redundant columns.
- Five declared columns contain small deterministic missing blocks.
