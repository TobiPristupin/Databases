#include <memory>
#include <iostream>
#include <cstring>


std::unique_ptr<int> recur(){
    return std::make_unique<int>(2);
}

struct tintin_vector {
    uint64_t data[2];
    uint64_t time_intervals[2];
    uint64_t interval_end_time[2];
    uint64_t size = 2;
};

struct tintin_scheduling_context {
    tintin_vector* count_vec;
};

uint64_t tintin_extrapolate_count_by_TAM(struct tintin_scheduling_context* sc, uint64_t time) {
    struct tintin_vector* tv;
    uint64_t rate_curr, rate_prev, recorded_interval_length, predicted_count_at_time, rate_slope, rate_at_time, extrapolated_interval_length;
    size_t n;

    tv = (struct tintin_vector* ) sc->count_vec;
    n = tv->size;
    while (n >= 0 && time < tv->interval_end_time[n-1]){
        n--;
    }

    if (n < 2){
        if (n == 0){
            return 0;
        }
        return tv->data[n-1];
    }

    // There is some tolerable loss of precision due to division here
    rate_curr = tv->data[n-1] / tv->time_intervals[n-1];
    rate_prev = tv->data[n-2] / tv->time_intervals[n-2];
    recorded_interval_length = tv->interval_end_time[n-1] - tv->interval_end_time[n-2];
    rate_slope = (rate_curr - rate_prev) / recorded_interval_length;
    rate_at_time = rate_curr + rate_slope * (time - tv->interval_end_time[n-1]);
    extrapolated_interval_length = time - tv->interval_end_time[n-2];
    predicted_count_at_time = (rate_prev + rate_at_time) * (extrapolated_interval_length) / 2;
    return predicted_count_at_time;
}

int main(){
    tintin_vector vector;
    vector.data[0] = 5;
    vector.data[1] = 10;
    vector.time_intervals[0] = 2;
    vector.time_intervals[1] = 2;
    vector.interval_end_time[0] = 6;
    vector.interval_end_time[1] = 11;

    tintin_scheduling_context sc;
    sc.count_vec = &vector;

    std::cout << tintin_extrapolate_count_by_TAM(&sc, 12) << "\n";
}