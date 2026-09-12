
        extern "C" __global__
        void score_bucket_plans_cap1_basic(
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
            long long* output,
            const int candidate_count,
            const int row_count,
            const long long expiry_ms,
            const int direction_code,
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
            long long wins = 0;
            long long losses = 0;
            long long ties = 0;
            long long invalid = 0;

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
                } else if (direction_code == 1) {
                    if (buy_win[row_index] != 0) {
                        wins += 1;
                    } else if (sell_win[row_index] != 0) {
                        losses += 1;
                    }
                } else {
                    if (sell_win[row_index] != 0) {
                        wins += 1;
                    } else if (buy_win[row_index] != 0) {
                        losses += 1;
                    }
                }
                active_due = release_ms;
            }

            const long long out = ((long long)candidate_index) * 8LL;
            output[out + 0] = total;
            output[out + 1] = wins;
            output[out + 2] = losses;
            output[out + 3] = ties;
            output[out + 4] = invalid;
            output[out + 5] = direction_code == 1 ? total : 0;
            output[out + 6] = direction_code == -1 ? total : 0;
            output[out + 7] = wins * payout_basis - losses * 100LL;
        }
        