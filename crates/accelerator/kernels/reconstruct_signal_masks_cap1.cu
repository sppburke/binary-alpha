
        extern "C" __global__
        void reconstruct_signal_masks_cap1(
            const short* feature_codes,
            const int* condition_feature,
            const short* condition_bucket,
            const int* candidate_offsets,
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

            const int condition_begin = candidate_offsets[candidate_index];
            const int condition_end = candidate_offsets[candidate_index + 1];
            long long active_due = -9223372036854775807LL;

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
        