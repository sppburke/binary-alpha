
        extern "C" __global__
        void score_bucket_plans_cap1_basic_dual(
            const short* feature_codes,
            const int* condition_feature,
            const short* condition_bucket,
            const int* candidate_offsets,
            const unsigned char* split_mask,
            const long long* ordered_rows,
            const long long* decision_time_ms,
            const long long* release_time_ms,
            const unsigned char* valid,
            const unsigned char* buy_win,
            const unsigned char* sell_win,
            const unsigned char* tie,
            long long* buy_output,
            long long* sell_output,
            const int candidate_count,
            const int row_count,
            const long long expiry_ms,
            const long long payout_basis
        ) {
            const int candidate_index = blockDim.x * blockIdx.x + threadIdx.x;
            if (candidate_index >= candidate_count) {
                return;
            }

            const int condition_begin = candidate_offsets[candidate_index];
            const int condition_end = candidate_offsets[candidate_index + 1];
            long long active_due = -9223372036854775807LL;
            long long total = 0;
            long long ties = 0;
            long long invalid = 0;
            long long buy_wins = 0;
            long long buy_losses = 0;
            long long sell_wins = 0;
            long long sell_losses = 0;

            for (int ordered_position = 0; ordered_position < row_count; ++ordered_position) {
                const int row_index = (int)ordered_rows[ordered_position];
                const unsigned char split_scope = split_mask[row_index];
                if (split_scope == 0) {
                    continue;
                }
                bool matches = true;
                for (int condition = condition_begin; condition < condition_end; ++condition) {
                    const int feature = condition_feature[condition];
                    const short bucket = condition_bucket[condition];
                    if (feature_codes[((long long)feature) * row_count + row_index] != bucket) {
                        matches = false;
                        break;
                    }
                }
                if (!matches) {
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
                if (valid[row_index] == 0) {
                    invalid += 1;
                } else if (tie[row_index] != 0) {
                    ties += 1;
                } else {
                    if (buy_win[row_index] != 0) {
                        buy_wins += 1;
                        sell_losses += 1;
                    } else if (sell_win[row_index] != 0) {
                        sell_wins += 1;
                        buy_losses += 1;
                    }
                }
                active_due = release_ms;
            }

            const long long out = ((long long)candidate_index) * 8LL;
            buy_output[out + 0] = total;
            buy_output[out + 1] = buy_wins;
            buy_output[out + 2] = buy_losses;
            buy_output[out + 3] = ties;
            buy_output[out + 4] = invalid;
            buy_output[out + 5] = total;
            buy_output[out + 6] = 0;
            buy_output[out + 7] = buy_wins * payout_basis - buy_losses * 100LL;
            sell_output[out + 0] = total;
            sell_output[out + 1] = sell_wins;
            sell_output[out + 2] = sell_losses;
            sell_output[out + 3] = ties;
            sell_output[out + 4] = invalid;
            sell_output[out + 5] = 0;
            sell_output[out + 6] = total;
            sell_output[out + 7] = sell_wins * payout_basis - sell_losses * 100LL;
        }
        