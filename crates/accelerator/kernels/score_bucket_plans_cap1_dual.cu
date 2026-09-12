
        extern "C" __global__
        void score_bucket_plans_cap1_dual(
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
            long long* buy_output,
            long long* sell_output,
            const int candidate_count,
            const int row_count,
            const int feature_count,
            const long long expiry_ms,
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
            long long ties = 0;
            long long invalid = 0;

            long long buy_wins = 0;
            long long buy_losses = 0;
            long long buy_equity = 0;
            long long buy_peak = 0;
            long long buy_max_drawdown = 0;
            long long buy_max_drawdown_trades = 0;
            double buy_drawdown_square_sum = 0.0;
            long long buy_trade_square_sum = 0;
            long long buy_downside_square_sum = 0;
            long long buy_underwater_start_trade = -1;
            long long buy_underwater_start_ms = -1;
            long long buy_peak_trade_index = 0;
            long long buy_peak_decision_ms = -1;
            long long buy_final_settlement_ms = -1;
            long long buy_valid_trade_index = 0;
            long long buy_longest_underwater = 0;
            long long buy_longest_underwater_ms = 0;
            long long buy_current_losses = 0;
            long long buy_max_losses = 0;
            long long buy_max_drawdown_ms = 0;
            long long buy_rolling[100];
            long long buy_rolling20 = 0;
            long long buy_rolling50 = 0;
            long long buy_rolling100 = 0;
            long long buy_worst20 = 9223372036854775807LL;
            long long buy_worst50 = 9223372036854775807LL;
            long long buy_worst100 = 9223372036854775807LL;

            long long sell_wins = 0;
            long long sell_losses = 0;
            long long sell_equity = 0;
            long long sell_peak = 0;
            long long sell_max_drawdown = 0;
            long long sell_max_drawdown_trades = 0;
            double sell_drawdown_square_sum = 0.0;
            long long sell_trade_square_sum = 0;
            long long sell_downside_square_sum = 0;
            long long sell_underwater_start_trade = -1;
            long long sell_underwater_start_ms = -1;
            long long sell_peak_trade_index = 0;
            long long sell_peak_decision_ms = -1;
            long long sell_final_settlement_ms = -1;
            long long sell_valid_trade_index = 0;
            long long sell_longest_underwater = 0;
            long long sell_longest_underwater_ms = 0;
            long long sell_current_losses = 0;
            long long sell_max_losses = 0;
            long long sell_max_drawdown_ms = 0;
            long long sell_rolling[100];
            long long sell_rolling20 = 0;
            long long sell_rolling50 = 0;
            long long sell_rolling100 = 0;
            long long sell_worst20 = 9223372036854775807LL;
            long long sell_worst50 = 9223372036854775807LL;
            long long sell_worst100 = 9223372036854775807LL;

            for (int ring_index = 0; ring_index < 100; ++ring_index) {
                buy_rolling[ring_index] = 0;
                sell_rolling[ring_index] = 0;
            }

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
                long long buy_trade_units = 0;
                long long sell_trade_units = 0;
                if (valid[row_index] == 0) {
                    invalid += 1;
                } else if (tie[row_index] != 0) {
                    ties += 1;
                } else {
                    if (buy_win[row_index] != 0) {
                        buy_wins += 1;
                        sell_losses += 1;
                        buy_trade_units = payout_basis;
                        sell_trade_units = -100LL;
                    } else if (sell_win[row_index] != 0) {
                        sell_wins += 1;
                        buy_losses += 1;
                        sell_trade_units = payout_basis;
                        buy_trade_units = -100LL;
                    }
                }

                if (valid[row_index] != 0) {
                    const long long settlement_ms = settlement_time_ms[row_index];
                    buy_final_settlement_ms = settlement_ms;
                    sell_final_settlement_ms = settlement_ms;
                    if (buy_peak_decision_ms < 0) buy_peak_decision_ms = decision_ms;
                    if (sell_peak_decision_ms < 0) sell_peak_decision_ms = decision_ms;
                    int ring_slot = (int)(buy_valid_trade_index % 100LL);
                    long long replaced = buy_rolling[ring_slot];
                    buy_rolling[ring_slot] = buy_trade_units;
                    buy_rolling100 += buy_trade_units - replaced;
                    buy_rolling50 += buy_trade_units;
                    buy_rolling20 += buy_trade_units;
                    if (buy_valid_trade_index >= 50LL) {
                        buy_rolling50 -= buy_rolling[(int)((buy_valid_trade_index - 50LL) % 100LL)];
                    }
                    if (buy_valid_trade_index >= 20LL) {
                        buy_rolling20 -= buy_rolling[(int)((buy_valid_trade_index - 20LL) % 100LL)];
                    }
                    if (buy_valid_trade_index >= 19LL && buy_rolling20 < buy_worst20) buy_worst20 = buy_rolling20;
                    if (buy_valid_trade_index >= 49LL && buy_rolling50 < buy_worst50) buy_worst50 = buy_rolling50;
                    if (buy_valid_trade_index >= 99LL && buy_rolling100 < buy_worst100) buy_worst100 = buy_rolling100;
                    buy_valid_trade_index += 1LL;
                    buy_equity += buy_trade_units;
                    buy_trade_square_sum += buy_trade_units * buy_trade_units;
                    if (buy_trade_units < 0) buy_downside_square_sum += buy_trade_units * buy_trade_units;
                    if (buy_trade_units < 0) {
                        buy_current_losses += 1;
                        if (buy_current_losses > buy_max_losses) buy_max_losses = buy_current_losses;
                    } else {
                        buy_current_losses = 0;
                    }
                    if (buy_equity >= buy_peak) {
                        if (buy_underwater_start_trade >= 0) {
                            const long long drawdown_length = buy_valid_trade_index - buy_peak_trade_index;
                            const long long drawdown_ms = settlement_ms - buy_peak_decision_ms;
                            if (drawdown_ms > buy_max_drawdown_ms || (drawdown_ms == buy_max_drawdown_ms && drawdown_length > buy_max_drawdown_trades)) {
                                buy_max_drawdown_ms = drawdown_ms;
                                buy_max_drawdown_trades = drawdown_length;
                            }
                            const long long underwater_length = buy_valid_trade_index - buy_underwater_start_trade;
                            const long long underwater_ms = settlement_ms - buy_underwater_start_ms;
                            if (underwater_length > buy_longest_underwater) buy_longest_underwater = underwater_length;
                            if (underwater_ms > buy_longest_underwater_ms) buy_longest_underwater_ms = underwater_ms;
                        }
                        if (buy_equity > buy_peak) {
                            buy_peak = buy_equity;
                        }
                        buy_peak_trade_index = buy_valid_trade_index;
                        buy_peak_decision_ms = settlement_ms;
                        buy_underwater_start_trade = -1;
                        buy_underwater_start_ms = -1;
                    }
                    long long drawdown = buy_peak - buy_equity;
                    buy_drawdown_square_sum += (double)drawdown * (double)drawdown;
                    if (drawdown > buy_max_drawdown) {
                        buy_max_drawdown = drawdown;
                    }
                    if (drawdown > 0) {
                        if (buy_underwater_start_trade < 0) {
                            buy_underwater_start_trade = buy_valid_trade_index;
                            buy_underwater_start_ms = settlement_ms;
                        }
                    }

                    ring_slot = (int)(sell_valid_trade_index % 100LL);
                    replaced = sell_rolling[ring_slot];
                    sell_rolling[ring_slot] = sell_trade_units;
                    sell_rolling100 += sell_trade_units - replaced;
                    sell_rolling50 += sell_trade_units;
                    sell_rolling20 += sell_trade_units;
                    if (sell_valid_trade_index >= 50LL) {
                        sell_rolling50 -= sell_rolling[(int)((sell_valid_trade_index - 50LL) % 100LL)];
                    }
                    if (sell_valid_trade_index >= 20LL) {
                        sell_rolling20 -= sell_rolling[(int)((sell_valid_trade_index - 20LL) % 100LL)];
                    }
                    if (sell_valid_trade_index >= 19LL && sell_rolling20 < sell_worst20) sell_worst20 = sell_rolling20;
                    if (sell_valid_trade_index >= 49LL && sell_rolling50 < sell_worst50) sell_worst50 = sell_rolling50;
                    if (sell_valid_trade_index >= 99LL && sell_rolling100 < sell_worst100) sell_worst100 = sell_rolling100;
                    sell_valid_trade_index += 1LL;
                    sell_equity += sell_trade_units;
                    sell_trade_square_sum += sell_trade_units * sell_trade_units;
                    if (sell_trade_units < 0) sell_downside_square_sum += sell_trade_units * sell_trade_units;
                    if (sell_trade_units < 0) {
                        sell_current_losses += 1;
                        if (sell_current_losses > sell_max_losses) sell_max_losses = sell_current_losses;
                    } else {
                        sell_current_losses = 0;
                    }
                    if (sell_equity >= sell_peak) {
                        if (sell_underwater_start_trade >= 0) {
                            const long long drawdown_length = sell_valid_trade_index - sell_peak_trade_index;
                            const long long drawdown_ms = settlement_ms - sell_peak_decision_ms;
                            if (drawdown_ms > sell_max_drawdown_ms || (drawdown_ms == sell_max_drawdown_ms && drawdown_length > sell_max_drawdown_trades)) {
                                sell_max_drawdown_ms = drawdown_ms;
                                sell_max_drawdown_trades = drawdown_length;
                            }
                            const long long underwater_length = sell_valid_trade_index - sell_underwater_start_trade;
                            const long long underwater_ms = settlement_ms - sell_underwater_start_ms;
                            if (underwater_length > sell_longest_underwater) sell_longest_underwater = underwater_length;
                            if (underwater_ms > sell_longest_underwater_ms) sell_longest_underwater_ms = underwater_ms;
                        }
                        if (sell_equity > sell_peak) {
                            sell_peak = sell_equity;
                        }
                        sell_peak_trade_index = sell_valid_trade_index;
                        sell_peak_decision_ms = settlement_ms;
                        sell_underwater_start_trade = -1;
                        sell_underwater_start_ms = -1;
                    }
                    drawdown = sell_peak - sell_equity;
                    sell_drawdown_square_sum += (double)drawdown * (double)drawdown;
                    if (drawdown > sell_max_drawdown) {
                        sell_max_drawdown = drawdown;
                    }
                    if (drawdown > 0) {
                        if (sell_underwater_start_trade < 0) {
                            sell_underwater_start_trade = sell_valid_trade_index;
                            sell_underwater_start_ms = settlement_ms;
                        }
                    }
                }

                active_due = release_ms;
            }

            if (buy_underwater_start_trade >= 0 && buy_final_settlement_ms >= 0) {
                const long long drawdown_length = buy_valid_trade_index - buy_peak_trade_index;
                const long long drawdown_ms = buy_final_settlement_ms - buy_peak_decision_ms;
                if (drawdown_ms > buy_max_drawdown_ms || (drawdown_ms == buy_max_drawdown_ms && drawdown_length > buy_max_drawdown_trades)) {
                    buy_max_drawdown_ms = drawdown_ms;
                    buy_max_drawdown_trades = drawdown_length;
                }
                const long long underwater_length = buy_valid_trade_index - buy_underwater_start_trade + 1LL;
                const long long underwater_ms = buy_final_settlement_ms - buy_underwater_start_ms;
                if (underwater_length > buy_longest_underwater) buy_longest_underwater = underwater_length;
                if (underwater_ms > buy_longest_underwater_ms) buy_longest_underwater_ms = underwater_ms;
            }
            if (sell_underwater_start_trade >= 0 && sell_final_settlement_ms >= 0) {
                const long long drawdown_length = sell_valid_trade_index - sell_peak_trade_index;
                const long long drawdown_ms = sell_final_settlement_ms - sell_peak_decision_ms;
                if (drawdown_ms > sell_max_drawdown_ms || (drawdown_ms == sell_max_drawdown_ms && drawdown_length > sell_max_drawdown_trades)) {
                    sell_max_drawdown_ms = drawdown_ms;
                    sell_max_drawdown_trades = drawdown_length;
                }
                const long long underwater_length = sell_valid_trade_index - sell_underwater_start_trade + 1LL;
                const long long underwater_ms = sell_final_settlement_ms - sell_underwater_start_ms;
                if (underwater_length > sell_longest_underwater) sell_longest_underwater = underwater_length;
                if (underwater_ms > sell_longest_underwater_ms) sell_longest_underwater_ms = underwater_ms;
            }

            const long long out = ((long long)candidate_index) * 21LL;
            buy_output[out + 0] = total;
            buy_output[out + 1] = buy_wins;
            buy_output[out + 2] = buy_losses;
            buy_output[out + 3] = ties;
            buy_output[out + 4] = invalid;
            buy_output[out + 5] = total;
            buy_output[out + 6] = 0;
            buy_output[out + 7] = buy_wins * payout_basis - buy_losses * 100LL;
            buy_output[out + 8] = buy_max_drawdown;
            buy_output[out + 9] = buy_max_losses;
            buy_output[out + 10] = __double_as_longlong(buy_drawdown_square_sum);
            buy_output[out + 11] = buy_longest_underwater;
            buy_output[out + 12] = buy_max_drawdown_trades;
            buy_output[out + 13] = buy_worst20 == 9223372036854775807LL ? 0 : buy_worst20;
            buy_output[out + 14] = buy_worst50 == 9223372036854775807LL ? 0 : buy_worst50;
            buy_output[out + 15] = buy_worst100 == 9223372036854775807LL ? 0 : buy_worst100;
            buy_output[out + 16] = buy_valid_trade_index;
            buy_output[out + 17] = buy_trade_square_sum;
            buy_output[out + 18] = buy_downside_square_sum;
            buy_output[out + 19] = buy_longest_underwater_ms;
            buy_output[out + 20] = buy_max_drawdown_ms;

            sell_output[out + 0] = total;
            sell_output[out + 1] = sell_wins;
            sell_output[out + 2] = sell_losses;
            sell_output[out + 3] = ties;
            sell_output[out + 4] = invalid;
            sell_output[out + 5] = 0;
            sell_output[out + 6] = total;
            sell_output[out + 7] = sell_wins * payout_basis - sell_losses * 100LL;
            sell_output[out + 8] = sell_max_drawdown;
            sell_output[out + 9] = sell_max_losses;
            sell_output[out + 10] = __double_as_longlong(sell_drawdown_square_sum);
            sell_output[out + 11] = sell_longest_underwater;
            sell_output[out + 12] = sell_max_drawdown_trades;
            sell_output[out + 13] = sell_worst20 == 9223372036854775807LL ? 0 : sell_worst20;
            sell_output[out + 14] = sell_worst50 == 9223372036854775807LL ? 0 : sell_worst50;
            sell_output[out + 15] = sell_worst100 == 9223372036854775807LL ? 0 : sell_worst100;
            sell_output[out + 16] = sell_valid_trade_index;
            sell_output[out + 17] = sell_trade_square_sum;
            sell_output[out + 18] = sell_downside_square_sum;
            sell_output[out + 19] = sell_longest_underwater_ms;
            sell_output[out + 20] = sell_max_drawdown_ms;
        }
        