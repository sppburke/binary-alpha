// One candidate walks its sparse driver list once for an expiry tile.
// Packed rows contain all releases as i64, then one flag byte per expiry;
// each row's stride is rounded up to eight bytes by the host.
extern "C" __global__ void score_screen_fused(
    const short* feature_codes,
    const int* condition_feature,
    const short* condition_bucket,
    const int* candidate_offsets,
    const int* candidate_driver_key,
    const int* key_chrono_offsets,
    const int* key_chrono_rows,
    const unsigned char* split_mask,
    const long long* entry_time_ms,
    const unsigned char* packed_outcomes,
    int* output,
    const int candidate_count,
    const int row_count,
    const int active_expiries,
    const int row_stride
) {
    const long long candidate_linear =
        (long long)blockIdx.x * blockDim.x + threadIdx.x;
    if (candidate_linear >= candidate_count) return;
    const int candidate = (int)candidate_linear;
    const int begin = candidate_offsets[candidate];
    const int end = candidate_offsets[candidate + 1];
    const int driver = candidate_driver_key[candidate];
    long long active_due[8];
    int total[8], buy_wins[8], sell_wins[8], ties[8], invalid[8];
#pragma unroll
    for (int expiry = 0; expiry < 8; ++expiry) {
        active_due[expiry] = -9223372036854775807LL;
        total[expiry] = buy_wins[expiry] = sell_wins[expiry] = 0;
        ties[expiry] = invalid[expiry] = 0;
    }
    if (driver >= 0) {
        for (int position = key_chrono_offsets[driver];
             position < key_chrono_offsets[driver + 1]; ++position) {
            const int row = key_chrono_rows[position];
            const unsigned char scope = split_mask[row];
            if (scope == 0) continue;
            bool matches = true;
            for (int condition = begin; condition < end; ++condition) {
                if (feature_codes[(long long)condition_feature[condition] * row_count + row]
                    != condition_bucket[condition]) {
                    matches = false;
                    break;
                }
            }
            if (!matches) continue;
            const long long decision = entry_time_ms[row];
            const unsigned char* packed = packed_outcomes + (long long)row * row_stride;
            const long long* releases = reinterpret_cast<const long long*>(packed);
            const unsigned char* flags = packed + active_expiries * 8;
#pragma unroll
            for (int expiry = 0; expiry < 8; ++expiry) {
                if (expiry >= active_expiries) continue;
                if (scope == 2) {
                    if (active_due[expiry] <= decision && releases[expiry] > 0)
                        active_due[expiry] = releases[expiry];
                    continue;
                }
                ++total[expiry];
                if (active_due[expiry] > decision || releases[expiry] <= 0) {
                    ++invalid[expiry];
                    continue;
                }
                const unsigned char flag = flags[expiry];
                if (!(flag & 1)) ++invalid[expiry];
                else if (flag & 2) ++ties[expiry];
                else if (flag & 4) ++buy_wins[expiry];
                else if (flag & 8) ++sell_wins[expiry];
                active_due[expiry] = releases[expiry];
            }
        }
    }
#pragma unroll
    for (int expiry = 0; expiry < 8; ++expiry) {
        if (expiry >= active_expiries) continue;
        const long long at = ((long long)candidate * active_expiries + expiry) * 5;
        output[at] = total[expiry];
        output[at + 1] = buy_wins[expiry];
        output[at + 2] = sell_wins[expiry];
        output[at + 3] = ties[expiry];
        output[at + 4] = invalid[expiry];
    }
}
