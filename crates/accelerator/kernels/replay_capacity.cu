
        extern "C" __global__
        void replay_capacity(
            const unsigned char* masks,
            const int* candidate_index,
            const long long* entry_time,
            const long long* due_time,
            const int* expiry_seconds,
            const unsigned char* hypothetical_valid,
            const unsigned char* standalone_admitted,
            unsigned char* accepted,
            const int portfolio_count,
            const int candidate_count,
            const int event_count,
            const int max_total,
            const int max_expiry
        ) {
            const int portfolio = blockDim.x * blockIdx.x + threadIdx.x;
            if (portfolio >= portfolio_count) return;
            long long open_due[64];
            int open_expiry[64];
            int open_count = 0;
            const long long output_offset = ((long long)portfolio) * event_count;
            const long long mask_offset = ((long long)portfolio) * candidate_count;
            for (int event = 0; event < event_count; ++event) {
                if (!hypothetical_valid[event] || !standalone_admitted[event]) continue;
                const int candidate = candidate_index[event];
                if (!masks[mask_offset + candidate]) continue;
                const long long entry = entry_time[event];
                int retained = 0;
                for (int index = 0; index < open_count; ++index) {
                    if (open_due[index] > entry) {
                        open_due[retained] = open_due[index];
                        open_expiry[retained] = open_expiry[index];
                        retained += 1;
                    }
                }
                open_count = retained;
                if (open_count >= max_total) continue;
                const int expiry = expiry_seconds[event];
                int expiry_count = 0;
                for (int index = 0; index < open_count; ++index) {
                    if (open_expiry[index] == expiry) expiry_count += 1;
                }
                if (expiry_count >= max_expiry) continue;
                accepted[output_offset + event] = 1;
                open_due[open_count] = due_time[event];
                open_expiry[open_count] = expiry;
                open_count += 1;
            }
        }
        