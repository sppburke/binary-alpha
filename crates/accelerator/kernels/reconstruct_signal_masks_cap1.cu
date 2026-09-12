
        extern "C" __global__
        void reconstruct_signal_masks_cap1(
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
            unsigned char* output,
            const int candidate_count,
            const int row_count,
            const long long expiry_ms
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
                if (active_due > decision_ms) {
                    continue;
                }
                const long long release_ms = release_time_ms[row_index];
                if (release_ms <= 0) {
                    continue;
                }
                output[((long long)candidate_index) * row_count + row_index] = 1;
                active_due = release_ms;
            }
        }
        