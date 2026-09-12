
        extern "C" __global__
        void bootstrap_path_metrics(
            const double* paths,
            double* max_drawdowns,
            long long* longest_underwater,
            long long* negative_rolling,
            const int simulations,
            const int trade_count,
            const int rolling_horizon
        ) {
            const int simulation = blockDim.x * blockIdx.x + threadIdx.x;
            if (simulation >= simulations) {
                return;
            }
            const double* path = paths + ((long long)simulation) * trade_count;
            double equity = 0.0;
            double peak = 0.0;
            double max_drawdown = 0.0;
            double rolling_sum = 0.0;
            long long underwater_run = 0;
            long long max_underwater_run = 0;
            long long negative_windows = 0;
            for (int index = 0; index < trade_count; ++index) {
                const double value = path[index];
                equity += value;
                if (equity >= peak) {
                    peak = equity;
                    underwater_run = 0;
                } else {
                    underwater_run += 1;
                    if (underwater_run > max_underwater_run) {
                        max_underwater_run = underwater_run;
                    }
                }
                const double drawdown = peak - equity;
                if (drawdown > max_drawdown) {
                    max_drawdown = drawdown;
                }

                rolling_sum += value;
                if (index >= rolling_horizon) {
                    rolling_sum -= path[index - rolling_horizon];
                }
                if (index + 1 >= rolling_horizon && rolling_sum < 0.0) {
                    negative_windows += 1;
                }
            }
            max_drawdowns[simulation] = max_drawdown;
            longest_underwater[simulation] = max_underwater_run;
            negative_rolling[simulation] = negative_windows;
        }
        