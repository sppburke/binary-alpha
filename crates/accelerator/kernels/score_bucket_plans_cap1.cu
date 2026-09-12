
        extern "C" __global__
        void score_bucket_plans_cap1(
            const short* feature_codes,
            const int* feature1,
            const short* bucket1,
            const int* feature2,
            const short* bucket2,
            const int* feature3,
            const short* bucket3,
            const int* feature4,
            const short* bucket4,
            const unsigned char* split_mask,
            const long long* ordered_rows,
            const long long* decision_time_ms,
            const long long* release_time_ms,
            const long long* settlement_time_ms,
            const unsigned char* valid,
            const unsigned char* buy_win,
            const unsigned char* sell_win,
            const unsigned char* tie,
            long long* output,
            const int candidate_count,
            const int row_count,
            const int feature_count,
            const long long expiry_ms,
            const int direction_code,
            const long long payout_basis
        ) {
            const int candidate_index = blockDim.x * blockIdx.x + threadIdx.x;
            if (candidate_index >= candidate_count) {
                return;
            }

            const int f1 = feature1[candidate_index];
            const int f2 = feature2[candidate_index];
            const int f3 = feature3[candidate_index];
            const int f4 = feature4[candidate_index];
            const short b1 = bucket1[candidate_index];
            const short b2 = bucket2[candidate_index];
            const short b3 = bucket3[candidate_index];
            const short b4 = bucket4[candidate_index];
            long long active_due = -9223372036854775807LL;
            long long total = 0;
            long long wins = 0;
            long long losses = 0;
            long long ties = 0;
            long long invalid = 0;
            long long equity = 0;
            long long peak = 0;
            long long max_drawdown = 0;
            long long max_drawdown_trades = 0;
            double drawdown_square_sum = 0.0;
            long long trade_square_sum = 0;
            long long downside_square_sum = 0;
            long long underwater_start_trade = -1;
            long long underwater_start_ms = -1;
            long long peak_trade_index = 0;
            long long peak_decision_ms = -1;
            long long final_settlement_ms = -1;
            long long valid_trade_index = 0;
            long long longest_underwater = 0;
            long long longest_underwater_ms = 0;
            long long current_losses = 0;
            long long max_losses = 0;
            long long max_drawdown_ms = 0;
            long long rolling[100];
            for (int ring_index = 0; ring_index < 100; ++ring_index) {
                rolling[ring_index] = 0;
            }
            long long rolling20 = 0;
            long long rolling50 = 0;
            long long rolling100 = 0;
            long long worst20 = 9223372036854775807LL;
            long long worst50 = 9223372036854775807LL;
            long long worst100 = 9223372036854775807LL;

            for (int ordered_position = 0; ordered_position < row_count; ++ordered_position) {
                const int row_index = (int)ordered_rows[ordered_position];
                const unsigned char split_scope = split_mask[row_index];
                if (split_scope == 0) {
                    continue;
                }
                if (feature_codes[((long long)f1) * row_count + row_index] != b1) {
                    continue;
                }
                if (f2 >= 0 && feature_codes[((long long)f2) * row_count + row_index] != b2) {
                    continue;
                }
                if (f3 >= 0 && feature_codes[((long long)f3) * row_count + row_index] != b3) {
                    continue;
                }
                if (f4 >= 0 && feature_codes[((long long)f4) * row_count + row_index] != b4) {
                    continue;
                }

                const long long decision_ms = decision_time_ms[row_index];
                if (split_scope == 2) {
                    if (active_due <= decision_ms) {
                        const long long warmup_release_ms = release_time_ms[row_index];
                        if (warmup_release_ms > 0) active_due = warmup_release_ms;
                    }
                    continue;
                }
                total += 1;
                if (active_due > decision_ms) {
                    invalid += 1;
                    continue;
                }
                const long long release_ms = release_time_ms[row_index];
                if (release_ms <= 0) {
                    invalid += 1;
                    continue;
                }
                long long trade_units = 0;
                if (valid[row_index] == 0) {
                    invalid += 1;
                } else if (tie[row_index] != 0) {
                    ties += 1;
                } else if (direction_code == 1) {
                    if (buy_win[row_index] != 0) {
                        wins += 1;
                        trade_units = payout_basis;
                    } else if (sell_win[row_index] != 0) {
                        losses += 1;
                        trade_units = -100LL;
                    }
                } else {
                    if (sell_win[row_index] != 0) {
                        wins += 1;
                        trade_units = payout_basis;
                    } else if (buy_win[row_index] != 0) {
                        losses += 1;
                        trade_units = -100LL;
                    }
                }

                if (valid[row_index] != 0) {
                    const int ring_slot = (int)(valid_trade_index % 100LL);
                    const long long replaced = rolling[ring_slot];
                    rolling[ring_slot] = trade_units;
                    rolling100 += trade_units - replaced;
                    rolling50 += trade_units;
                    rolling20 += trade_units;
                    if (valid_trade_index >= 50LL) {
                        rolling50 -= rolling[(int)((valid_trade_index - 50LL) % 100LL)];
                    }
                    if (valid_trade_index >= 20LL) {
                        rolling20 -= rolling[(int)((valid_trade_index - 20LL) % 100LL)];
                    }
                    if (valid_trade_index >= 19LL && rolling20 < worst20) {
                        worst20 = rolling20;
                    }
                    if (valid_trade_index >= 49LL && rolling50 < worst50) {
                        worst50 = rolling50;
                    }
                    if (valid_trade_index >= 99LL && rolling100 < worst100) {
                        worst100 = rolling100;
                    }
                    valid_trade_index += 1LL;
                    const long long settlement_ms = settlement_time_ms[row_index];
                    final_settlement_ms = settlement_ms;
                    if (peak_decision_ms < 0) {
                        peak_decision_ms = decision_ms;
                    }
                    equity += trade_units;
                    trade_square_sum += trade_units * trade_units;
                    if (trade_units < 0) {
                        downside_square_sum += trade_units * trade_units;
                    }
                    if (trade_units < 0) {
                        current_losses += 1;
                        if (current_losses > max_losses) max_losses = current_losses;
                    } else {
                        current_losses = 0;
                    }
                    if (equity >= peak) {
                        if (underwater_start_trade >= 0) {
                            const long long drawdown_length = valid_trade_index - peak_trade_index;
                            const long long drawdown_ms = settlement_ms - peak_decision_ms;
                            if (drawdown_ms > max_drawdown_ms || (drawdown_ms == max_drawdown_ms && drawdown_length > max_drawdown_trades)) {
                                max_drawdown_ms = drawdown_ms;
                                max_drawdown_trades = drawdown_length;
                            }
                            const long long underwater_length = valid_trade_index - underwater_start_trade;
                            const long long underwater_ms = settlement_ms - underwater_start_ms;
                            if (underwater_length > longest_underwater) longest_underwater = underwater_length;
                            if (underwater_ms > longest_underwater_ms) longest_underwater_ms = underwater_ms;
                        }
                        if (equity > peak) {
                            peak = equity;
                        }
                        peak_trade_index = valid_trade_index;
                        peak_decision_ms = settlement_ms;
                        underwater_start_trade = -1;
                        underwater_start_ms = -1;
                    }
                    const long long drawdown = peak - equity;
                    drawdown_square_sum += (double)drawdown * (double)drawdown;
                    if (drawdown > max_drawdown) {
                        max_drawdown = drawdown;
                    }
                    if (drawdown > 0) {
                        if (underwater_start_trade < 0) {
                            underwater_start_trade = valid_trade_index;
                            underwater_start_ms = settlement_ms;
                        }
                    }
                }

                active_due = release_ms;
            }

            if (underwater_start_trade >= 0 && final_settlement_ms >= 0) {
                const long long drawdown_length = valid_trade_index - peak_trade_index;
                const long long drawdown_ms = final_settlement_ms - peak_decision_ms;
                if (drawdown_ms > max_drawdown_ms || (drawdown_ms == max_drawdown_ms && drawdown_length > max_drawdown_trades)) {
                    max_drawdown_ms = drawdown_ms;
                    max_drawdown_trades = drawdown_length;
                }
                const long long underwater_length = valid_trade_index - underwater_start_trade + 1LL;
                const long long underwater_ms = final_settlement_ms - underwater_start_ms;
                if (underwater_length > longest_underwater) longest_underwater = underwater_length;
                if (underwater_ms > longest_underwater_ms) longest_underwater_ms = underwater_ms;
            }

            const long long out = ((long long)candidate_index) * 21LL;
            output[out + 0] = total;
            output[out + 1] = wins;
            output[out + 2] = losses;
            output[out + 3] = ties;
            output[out + 4] = invalid;
            output[out + 5] = direction_code == 1 ? total : 0;
            output[out + 6] = direction_code == -1 ? total : 0;
            output[out + 7] = wins * payout_basis - losses * 100LL;
            output[out + 8] = max_drawdown;
            output[out + 9] = max_losses;
            output[out + 10] = __double_as_longlong(drawdown_square_sum);
            output[out + 11] = longest_underwater;
            output[out + 12] = max_drawdown_trades;
            output[out + 13] = worst20 == 9223372036854775807LL ? 0 : worst20;
            output[out + 14] = worst50 == 9223372036854775807LL ? 0 : worst50;
            output[out + 15] = worst100 == 9223372036854775807LL ? 0 : worst100;
            output[out + 16] = valid_trade_index;
            output[out + 17] = trade_square_sum;
            output[out + 18] = downside_square_sum;
            output[out + 19] = longest_underwater_ms;
            output[out + 20] = max_drawdown_ms;
        }
        