# Deterministic sample shape list

Base seed: `20261120`; dtype: `float64`; generator: `scripts/generate_samples.py`.

| name | mode | logical M,N,K | batch | A shape | B shape | flags | nonfinite |
|---|---|---:|---:|---|---|---|---|
| small_64x64x64 | gemm | 64x64x64 | 1 | [64, 64] | [64, 64] | N/N | no |
| medium_512x256x384 | gemm | 512x256x384 | 1 | [512, 384] | [384, 256] | N/N | no |
| large_1024x1024x1024 | gemm | 1024x1024x1024 | 1 | [1024, 1024] | [1024, 1024] | N/N | no |
| nonsquare_128x96x80 | gemm | 128x96x80 | 1 | [128, 80] | [80, 96] | N/N | no |
| trans_a_96x128x64 | gemm | 96x128x64 | 1 | [64, 96] | [64, 128] | T/N | no |
| trans_b_80x64x96 | gemm | 80x64x96 | 1 | [80, 96] | [64, 96] | N/T | no |
| trans_ab_72x88x64 | gemm | 72x88x64 | 1 | [64, 72] | [88, 64] | T/T | no |
| batch_4x32x24x40 | bmm | 32x24x40 | 4 | [4, 32, 40] | [4, 40, 24] | N/N | no |
| batch_trans_3x17x29x23 | bmm | 17x29x23 | 3 | [3, 23, 17] | [3, 29, 23] | T/T | no |
| anomaly_nan_inf_48x40x36 | gemm | 48x40x36 | 1 | [48, 36] | [36, 40] | N/N | yes |
| batch_anomaly_2x12x10x8 | bmm | 12x10x8 | 2 | [2, 12, 8] | [2, 8, 10] | N/N | yes |
