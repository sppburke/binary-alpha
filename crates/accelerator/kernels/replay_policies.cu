
extern "C" __global__
void replay_policies(
    const long long* candidate_offsets,
    const long long* entry_ms,
    const long long* settlement_ms,
    const long long* close_ms,
    const signed char* outcomes,
    const unsigned char* valid_entry,
    const unsigned long long* failure_words,
    const int word_count,
    const int* policy_candidate,
    const unsigned long long* policy_masks,
    const int policy_count,
    const long long start_ms,
    const long long end_ms,
    const long long payout_fp,
    int* out_opened,
    int* out_settled,
    int* out_wins,
    int* out_losses,
    int* out_ties,
    long long* out_net_fp,
    long long* out_max_dd_fp,
    long long* out_longest_dd_ms,
    int* out_longest_dd_trades,
    int* out_longest_loss_streak,
    long long* out_gross_profit_fp,
    long long* out_gross_loss_fp,
    long long* out_sum_returns_fp,
    long long* out_sum_squares_fp2,
    long long* out_downside_squares_fp2
) {
    const int policy = blockDim.x * blockIdx.x + threadIdx.x;
    if (policy >= policy_count) return;
    const int candidate = policy_candidate[policy];
    const long long first = candidate_offsets[candidate];
    const long long last = candidate_offsets[candidate + 1];
    long long open_event = -1;
    int opened = 0, settled = 0, wins = 0, losses = 0, ties = 0;
    int loss_streak = 0, longest_loss_streak = 0;
    long long net = 0, peak = 0, max_dd = 0;
    long long gross_profit = 0, gross_loss = 0;
    long long sum_squares = 0, downside_squares = 0;
    long long dd_start_ms = 0, last_settle_ms = 0, peak_time_ms = 0, curve_start_ms = 0;
    int dd_start_trade = 0, peak_trade = 0, longest_dd_trades = 0;
    long long longest_dd_ms = 0;

    #define SETTLE_EVENT(EVENT_INDEX) \
        do { \
            const signed char oc = outcomes[(EVENT_INDEX)]; \
            const long long st = settlement_ms[(EVENT_INDEX)]; \
            if (oc >= 0 && st > 0 && st < end_ms) { \
                const long long value = oc == 1 ? payout_fp : (oc == 0 ? -10000LL : 0LL); \
                settled += 1; last_settle_ms = st; net += value; sum_squares += value * value; \
                if (value > 0) { wins += 1; gross_profit += value; loss_streak = 0; } \
                else if (value < 0) { losses += 1; gross_loss += -value; downside_squares += value * value; loss_streak += 1; if (loss_streak > longest_loss_streak) longest_loss_streak = loss_streak; } \
                else { ties += 1; loss_streak = 0; } \
                if (net >= peak) { \
                    if (dd_start_ms > 0) { const long long duration = st - dd_start_ms; const int trades = settled - dd_start_trade; if (duration > longest_dd_ms) longest_dd_ms = duration; if (trades > longest_dd_trades) longest_dd_trades = trades; } \
                    if (net > peak) { peak = net; peak_time_ms = st; peak_trade = settled; } dd_start_ms = 0; \
                } else { \
                    const long long dd = peak - net; if (dd > max_dd) max_dd = dd; \
                    if (dd > 0 && dd_start_ms == 0) { dd_start_ms = peak_time_ms > 0 ? peak_time_ms : (curve_start_ms > 0 ? curve_start_ms : st); dd_start_trade = peak_trade; } \
                } \
            } else if (oc < 0 && close_ms[(EVENT_INDEX)] > 0 && opened > 0) { opened -= 1; } \
        } while (0)

    for (long long event = first; event < last; ++event) {
        const long long entry = entry_ms[event];
        if (entry < start_ms) continue;
        if (entry >= end_ms) break;
        if (open_event >= 0 && close_ms[open_event] > 0 && close_ms[open_event] <= entry) {
            SETTLE_EVENT(open_event);
            open_event = -1;
        }
        if (!valid_entry[event]) continue;
        bool blocked_by_policy = false;
        for (int word = 0; word < word_count; ++word) {
            if ((failure_words[event * word_count + word] & policy_masks[policy * word_count + word]) != 0ULL) {
                blocked_by_policy = true;
                break;
            }
        }
        if (blocked_by_policy || open_event >= 0) continue;
        open_event = event;
        if (curve_start_ms == 0) curve_start_ms = entry;
        opened += 1;
    }
    if (open_event >= 0) SETTLE_EVENT(open_event);
    if (dd_start_ms > 0 && last_settle_ms > 0) {
        const long long duration = last_settle_ms - dd_start_ms;
        const int trades = settled - dd_start_trade;
        if (duration > longest_dd_ms) longest_dd_ms = duration;
        if (trades > longest_dd_trades) longest_dd_trades = trades;
    }
    out_opened[policy] = opened; out_settled[policy] = settled;
    out_wins[policy] = wins; out_losses[policy] = losses; out_ties[policy] = ties;
    out_net_fp[policy] = net; out_max_dd_fp[policy] = max_dd;
    out_longest_dd_ms[policy] = longest_dd_ms; out_longest_dd_trades[policy] = longest_dd_trades;
    out_longest_loss_streak[policy] = longest_loss_streak;
    out_gross_profit_fp[policy] = gross_profit; out_gross_loss_fp[policy] = gross_loss;
    out_sum_returns_fp[policy] = net; out_sum_squares_fp2[policy] = sum_squares;
    out_downside_squares_fp2[policy] = downside_squares;
    #undef SETTLE_EVENT
}
