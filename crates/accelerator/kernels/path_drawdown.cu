
        extern "C" __global__
        void path_drawdown(
            const float* returns,
            double* max_drawdown,
            double* ulcer_index,
            const int observation_count,
            const int portfolio_count
        ) {
            const int portfolio = blockDim.x * blockIdx.x + threadIdx.x;
            if (portfolio >= portfolio_count) return;
            double equity = 0.0;
            double peak = 0.0;
            double largest = 0.0;
            double square_sum = 0.0;
            for (int observation = 0; observation < observation_count; ++observation) {
                equity += (double)returns[((long long)observation) * portfolio_count + portfolio];
                if (equity > peak) peak = equity;
                const double drawdown = peak - equity;
                if (drawdown > largest) largest = drawdown;
                square_sum += drawdown * drawdown;
            }
            max_drawdown[portfolio] = largest;
            ulcer_index[portfolio] = observation_count > 0 ? sqrt(square_sum / observation_count) : 0.0;
        }
        